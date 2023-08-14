package org.apache.cassandra.spark.validation;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StartupValidation
{
    private static final Logger LOGGER = LoggerFactory.getLogger(StartupValidation.class);
    private static final StartupValidation INSTANCE = new StartupValidation();

    private final List<StartupValidator> validators = new ArrayList<>();

    private StartupValidation()
    {
    }

    public void register(StartupValidator validator)
    {
        validators.add(validator);
    }

    public void perform()
    {
        LOGGER.info("Performing startup validations");
        validators.forEach(StartupValidator::perform);
        LOGGER.info("Completed startup validations");
    }

    public static StartupValidation instance()
    {
        return INSTANCE;
    }
}
