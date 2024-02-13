package org.apache.cassandra.sidecar.testing;

import java.util.function.Predicate;

import org.apache.cassandra.distributed.impl.AbstractCluster;

/**
 * Predicate to instruct the JVM DTest framework on the classes should be loaded by the shared classloader.
 */
public class JvmDTestSharedClassesPredicate implements Predicate<String>
{
    public static final JvmDTestSharedClassesPredicate INSTANCE = new JvmDTestSharedClassesPredicate();

    private static final Predicate<String> EXTRA = className -> {
        // Those classes can be reached by Spark SizeEstimator, when it estimates the broadcast variable.
        // In the test scenario containing cassandra instance shutdown, there is a chance that it pick the class
        // that is loaded by the closed instance classloader, causing the following exception.
        // java.lang.IllegalStateException: Can't load <CLASS>. Instance class loader is already closed.
        return className.equals("org.apache.cassandra.utils.concurrent.Ref$OnLeak")
               || className.startsWith("org.apache.cassandra.metrics.RestorableMeter");
    };
    private static final Predicate<String> DELEGATE = AbstractCluster.SHARED_PREDICATE.or(EXTRA);

    @Override
    public boolean test(String s)
    {
        return DELEGATE.test(s);
    }
}
