package org.apache.cassandra.spark.validation;

public class TestValidation implements StartupValidation
{
    private final boolean succeed;

    public static TestValidation failing()
    {
        return new TestValidation(false);
    }

    public static TestValidation succeeding()
    {
        return new TestValidation(true);
    }

    private TestValidation(boolean succeed)
    {
        this.succeed = succeed;
    }

    @Override
    public void validate()
    {
        if (!succeed)
        {
            throw new RuntimeException("Failing test validation");
        }
    }
}
