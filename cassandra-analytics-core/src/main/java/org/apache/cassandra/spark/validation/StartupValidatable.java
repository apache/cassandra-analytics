package org.apache.cassandra.spark.validation;

public interface StartupValidatable
{
    void register(StartupValidation validation);
}
