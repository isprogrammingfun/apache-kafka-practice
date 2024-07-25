package streams.processor

import org.apache.kafka.streams.processor.Processor
import org.apache.kafka.streams.processor.ProcessorContext

class FilterProcessor : Processor<Any, Any> {
    private lateinit var context: ProcessorContext

    override fun init(context: ProcessorContext) {
        this.context = context
    }

    override fun process(key: Any?, value: Any?) {
        if (value.toString().length > 5) {
            context.forward(key, value)
        }
    }

    override fun close() {
    }

}