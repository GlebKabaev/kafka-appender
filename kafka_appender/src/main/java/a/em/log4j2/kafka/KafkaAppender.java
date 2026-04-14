package a.em.log4j2.kafka;

import org.apache.logging.log4j.core.*;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.layout.PatternLayout;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

@Plugin(name = "KafkaAppender", category = Core.CATEGORY_NAME, elementType = Appender.ELEMENT_TYPE, printObject = true)
public class KafkaAppender extends AbstractAppender {
    private final KafkaProducerManager kafkaManager;

    KafkaAppender(String name, Filter filter, Layout<? extends Serializable> layout,
                  boolean ignoreExceptions, KafkaProducerManager kafkaManager) {
        super(name, filter, layout, ignoreExceptions, Property.EMPTY_ARRAY);
        this.kafkaManager = kafkaManager;
    }

    @Override
    public void append(LogEvent event) {
        String message = new String(getLayout().toByteArray(event));
        kafkaManager.send(message);
    }

    @PluginFactory
    public static KafkaAppender createAppender(
            @PluginAttribute("name") String name,
            @PluginAttribute("topic") String topic,
            @PluginAttribute("bootstrapServers") String bootstrapServers,
            @PluginElement("Layout") Layout<? extends Serializable> layout,
            @PluginElement("Filter") Filter filter) {

        if (name == null || topic == null || bootstrapServers == null) {
            LOGGER.error("KafkaAppender: name, topic and bootstrapServers must be specified.");
            return null;
        }
        Layout<? extends Serializable> finalLayout = (layout == null)
                ? PatternLayout.createDefaultLayout()
                : layout;

        KafkaProducerManager manager = new KafkaProducerManager(bootstrapServers, topic);

        return new KafkaAppender(name, filter, finalLayout, true, manager);
    }

    @Override
    public boolean stop(long timeout, TimeUnit timeUnit) {
        setStopping();
        boolean stopped = super.stop(timeout, timeUnit);
        kafkaManager.close(timeout, timeUnit);
        setStopped();
        return stopped;
    }
}
