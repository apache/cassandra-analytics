/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.sidecar.testing;

import java.util.Map;

import com.google.inject.Singleton;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.User;
import io.vertx.ext.auth.authentication.AuthenticationProvider;
import io.vertx.ext.auth.authentication.Credentials;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.impl.AuthenticationHandlerImpl;
import io.vertx.ext.web.handler.impl.AuthenticationHandlerInternal;
import org.apache.cassandra.sidecar.acl.authentication.AuthenticationHandlerFactory;
import org.apache.cassandra.sidecar.config.AccessControlConfiguration;
import org.apache.cassandra.sidecar.exceptions.ConfigurationException;
import org.apache.cassandra.sidecar.metrics.server.AuthMetrics;

@Singleton
public class TestAuthenticationHandlerFactory implements AuthenticationHandlerFactory
{
    @Override
    public AuthenticationHandlerInternal create(Vertx vertx,
                                                AccessControlConfiguration accessControlConfiguration,
                                                Map<String, String> parameters,
                                                AuthMetrics metrics) throws ConfigurationException
    {
        return new TestAuthHandler(new NoOpAuthentication(), "cassandra", "secret");
    }

    public static class TestAuthHandler extends AuthenticationHandlerImpl<NoOpAuthentication>
    {
        static final String USERNAME_HTTP_HEADER = "AUTH_USER";
        static final String PASSWORD_HTTP_HEADER = "AUTH_PASS";

        private final String authUser;
        private final String authPass;

        public TestAuthHandler(NoOpAuthentication authProvider, String authUser, String authPass)
        {
            super(authProvider);
            this.authUser = authUser;
            this.authPass = authPass;
        }

        public void authenticate(RoutingContext routingContext, Handler<AsyncResult<User>> handler)
        {
            MultiMap requestHeaders = routingContext.request().headers();
            if (authUser.equals(requestHeaders.get(USERNAME_HTTP_HEADER)) && authPass.equals(requestHeaders.get(PASSWORD_HTTP_HEADER)))
            {
                handler.handle(Future.succeededFuture(User.fromName("dummy")));
                return;
            }
            handler.handle(Future.failedFuture(new RuntimeException("dummy")));
        }
    }

    public static class NoOpAuthentication implements AuthenticationProvider
    {
        @Override
        public Future<User> authenticate(Credentials credentials)
        {
            return Future.succeededFuture(User.fromName("dummy"));
        }

        @Override
        @Deprecated
        public void authenticate(JsonObject credentials, Handler<AsyncResult<User>> resultHandler)
        {
            throw new UnsupportedOperationException();
        }
    }
}
