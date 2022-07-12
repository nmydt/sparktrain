import org.apache.log4j.Logger;

/**
 * Description: 模拟日志产生
 */

public class LoggerGenerate {

    private static final Logger logger = Logger.getLogger(LoggerGenerate.class.getName());

    public static void main(String[] args) throws Exception {

        int index = 0;

        while (true) {
            Thread.sleep(1000);
            logger.info("value:" + index++);
        }
    }
}
