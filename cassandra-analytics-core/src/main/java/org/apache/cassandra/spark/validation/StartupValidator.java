package org.apache.cassandra.spark.validation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@FunctionalInterface
public interface StartupValidator
{
    Logger LOGGER = LoggerFactory.getLogger(StartupValidator.class);

    void validate();

    default void perform()
    {
        try
        {
            LOGGER.info("Performing startup validation with " + getClass());
            validate();
        }
        catch (Throwable throwable)
        {
            String message = "Failed startup validation with " + getClass();
            LOGGER.error(message, throwable);
            throw new RuntimeException(message, throwable);
        }
    }
}
