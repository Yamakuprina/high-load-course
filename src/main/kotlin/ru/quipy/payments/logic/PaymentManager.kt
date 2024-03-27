package ru.quipy.payments.logic

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.config.ExternalServicesConfig
import ru.quipy.payments.subscribers.OrderPaymentSubscriber
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import javax.annotation.PostConstruct
import kotlin.collections.ArrayList

@Service
open class PaymentManager(
) {

    val logger: Logger = LoggerFactory.getLogger(OrderPaymentSubscriber::class.java)

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    private val paymentExecutor = Executors.newFixedThreadPool(16, NamedThreadFactory("payment-executor"))
    private var services = ArrayList<PaymentExternalServiceImpl>()

    @PostConstruct
    fun init() {
        ExternalServicesConfig.getAccounts().forEach { property ->
            val serviceImpl = PaymentExternalServiceImpl(property, paymentESService)
            services.add(serviceImpl)
            serviceImpl.startListening();
        }
    }

    fun processPayment(paymentId: UUID, orderId: UUID, amount: Int, createdAt: Long) {
        paymentExecutor.submit {
            val createdEvent = paymentESService.create {
                it.create(
                    paymentId,
                    orderId,
                    amount
                )
            }
            logger.info("Payment ${createdEvent.paymentId} for order $orderId created.")

            val timePassed = (System.currentTimeMillis() - createdAt) / 1000f
            var isProcessed = false;
            services.stream().filter {
                it.getQueueProcessTime() + timePassed < it.getTimeOut().toMillis() / 1000
            }
                .min(Comparator.comparing { it.getProperties().cost }).ifPresent {
                    it.addToQueue(Runnable {
                        it.submitPaymentRequest(paymentId, amount, createdAt)
                    })

                    isProcessed = true
                }

            if (!isProcessed) {
                val transactionId = UUID.randomUUID()
                paymentESService.update(paymentId) {
                    it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - createdAt))
                }

                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "No free services available")
                }
            }
        }
    }
}