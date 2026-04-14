package a.em;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
public class LogProducerApp {

    private static final Logger log = LogManager.getLogger(LogProducerApp.class);

    public static void main(String[] args) throws Exception {
        log.info("Приложение стартовало");
        Thread.sleep(500);

        log.info("Сообщение номер 1");
        Thread.sleep(500);

        log.warn("Сообщение номер 2");
        Thread.sleep(500);

        log.error("Сообщение номер 3");
        Thread.sleep(500);

        log.info("Приложение завершилось");
    }
}