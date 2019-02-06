/*
 * Copyright 2018 The Hyve
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.radarcns.util;

import java.security.GeneralSecurityException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import okhttp3.internal.platform.Platform;
import okhttp3.internal.tls.OkHostnameVerifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility methods and variables for OkHttp initialization. */
public final class RestUtils {
    private static final Logger logger = LoggerFactory.getLogger(RestUtils.class);

    /** OkHttp3 default hostname verifier. */
    public static final HostnameVerifier DEFAULT_HOSTNAME_VERIFIER = OkHostnameVerifier.INSTANCE;
    /** OkHttp3 hostname verifier for unsafe connections. */
    public static final HostnameVerifier UNSAFE_HOSTNAME_VERIFIER = new HostnameVerifier() {
        @Override
        public boolean verify(String hostname, SSLSession session) {
            return true;
        }
    };

    /** Unsafe OkHttp3 trust manager that trusts all certificates. */
    public static final TrustManager[] UNSAFE_TRUST_MANAGER = {
            new X509TrustManager() {
                @Override
                public void checkClientTrusted(java.security.cert.X509Certificate[] chain,
                        String authType) {
                    //Nothing to do
                }

                @Override
                public void checkServerTrusted(java.security.cert.X509Certificate[] chain,
                        String authType) {
                    //Nothing to do
                }

                @Override
                public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                    return new java.security.cert.X509Certificate[]{};
                }
            }
    };

    /** Unsafe OkHttp3 SSLSocketFactory that trusts all certificates. */
    public static final SSLSocketFactory UNSAFE_SSL_FACTORY;

    static {
        SSLSocketFactory factory;
        try {
            final SSLContext sslContext = SSLContext.getInstance("SSL");
            sslContext.init(null, UNSAFE_TRUST_MANAGER, new java.security.SecureRandom());

            factory = sslContext.getSocketFactory();
        } catch (NoSuchAlgorithmException | KeyManagementException e) {
            logger.error("Failed to initialize unsafe SSL factory", e);
            factory = null;
        }
        UNSAFE_SSL_FACTORY = factory;
    }


    private RestUtils() {
        // utility class
    }

    /**
     * Default OkHttp3 trust manager that trusts all certificates.
     * Copied from private method in OkHttpClient.
     */
    public static X509TrustManager systemDefaultTrustManager() {
        try {
            TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(
                    TrustManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init((KeyStore) null);
            TrustManager[] trustManagers = trustManagerFactory.getTrustManagers();
            if (trustManagers.length != 1 || !(trustManagers[0] instanceof X509TrustManager)) {
                throw new IllegalStateException("Unexpected default trust managers:"
                        + Arrays.toString(trustManagers));
            }
            return (X509TrustManager) trustManagers[0];
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("No System TLS", e);
        }
    }

    /**
     * Default OkHttp3 SSLSocketFactory that trusts all certificates.
     * Copied from private method in OkHttpClient.
     */
    public static SSLSocketFactory systemDefaultSslSocketFactory(X509TrustManager trustManager) {
        try {
            SSLContext sslContext = Platform.get().getSSLContext();
            sslContext.init(null, new TrustManager[] { trustManager }, null);
            return sslContext.getSocketFactory();
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("No System TLS", e);
        }
    }
}
