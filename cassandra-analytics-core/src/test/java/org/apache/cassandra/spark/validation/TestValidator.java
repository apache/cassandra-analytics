package org.apache.cassandra.spark.validation;

public class TestValidator implements StartupValidator
{
    private final boolean succeed;

    public static TestValidator failing()
    {
        return new TestValidator(false);
    }

    public static TestValidator succeeding()
    {
        return new TestValidator(true);
    }

    private TestValidator(boolean succeed)
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
