package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.*
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.RateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.BlockingQueue
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import kotlin.math.min


// Advice: always treat time as a Duration
class PaymentExternalServiceImpl(
    private val properties: ExternalServiceProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalService {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalServiceImpl::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()

    }

    private val paymentOperationTimeout = Duration.ofSeconds(80)
    private val paymentsQueue: BlockingQueue<Runnable> = LinkedBlockingQueue()
    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.request95thPercentileProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests
    private val speed = min(parallelRequests.toFloat() / requestAverageProcessingTime.seconds.toFloat(), rateLimitPerSec.toFloat())
    private val rateLimiter = RateLimiter(rateLimitPerSec);
    private val window = OngoingWindow(parallelRequests);

    private val httpResponseExecutor = Executors.newFixedThreadPool(100,
        NamedThreadFactory("http"))

    private val httpClientExecutor = Executors.newFixedThreadPool(100,
        NamedThreadFactory("http-client"))

    private val client = OkHttpClient.Builder().run {
        dispatcher(Dispatcher(httpClientExecutor).apply { maxRequests = 5000; maxRequestsPerHost = 5000 })
        protocols(listOf(Protocol.H2_PRIOR_KNOWLEDGE))
        build()
    }

    private val paymentExecutor = Executors.newFixedThreadPool(100,
        NamedThreadFactory("payment"))

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long) {

        val transactionId = UUID.randomUUID()

        logger.warn("[$accountName] Submitting payment request for payment $paymentId. Already passed: ${now() - paymentStartedAt} ms")
        logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        val request = Request.Builder().run {
            url("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId")
            post(emptyBody)
        }.build()

        if (requestAverageProcessingTime + Duration.ofMillis(now() - paymentStartedAt)
            > paymentOperationTimeout) {
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
            }
            return
        }

        client.newCall(request).enqueue(
            object: Callback {
                override fun onFailure(call: Call, e: IOException) {
                    httpResponseExecutor.submit{
                        onFailure(call, e, transactionId, paymentId, properties)
                    }
                }

                override fun onResponse(call: Call, response: Response) {
                    httpResponseExecutor.submit{
                        onResponse(call, response, properties, transactionId, paymentId)
                    }
                }
            }
        )
    }

    fun onFailure(call: Call, e: IOException, transactionId: UUID, paymentId: UUID, properties: ExternalServiceProperties) {
        window.release()
        logger.error("Request timeout!")
        when (e) {
            is SocketTimeoutException -> {
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                }
            }

            else -> {
                logger.error(
                    "[${properties.accountName}] Payment failed for txId: $transactionId, payment: $paymentId",
                )
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = e.message)
                }
            }
        }
    }

    fun onResponse(call: Call, response: Response, properties: ExternalServiceProperties, transactionId: UUID, paymentId: UUID) {
        window.release()
        val body = try {
            mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
        } catch (e: Exception) {
            logger.error("[${properties.accountName}] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
            ExternalSysResponse(false, e.message)
        }

        logger.warn("[$properties.accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

        // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
        // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
        paymentESService.update(paymentId) {
            it.logProcessing(body.result, now(), transactionId, reason = body.message)
        }
    }

    fun startListening() {
        paymentExecutor.submit {
            while (true) {
                window.acquire()
                rateLimiter.tickBlocking()

                val task = paymentsQueue.take()
                paymentExecutor.execute(task)
            }
        }
    }

    fun addToQueue(task: Runnable) {
        paymentsQueue.put(task)
    }

    fun getQueueProcessTime(): Int {
        return ((paymentsQueue.size + 1) / speed).toInt();
    }

    fun getPaymentESService(): EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState> {
        return paymentESService
    }


    fun getProperties(): ExternalServiceProperties {
        return properties
    }

    fun getTimeOut(): Duration {
        return paymentOperationTimeout;
    }
}

public fun now() = System.currentTimeMillis()