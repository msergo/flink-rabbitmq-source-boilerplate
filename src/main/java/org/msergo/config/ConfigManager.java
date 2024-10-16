package org.msergo.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class ConfigManager {
    private static final String ENV = System.getenv("ENV");

    public static Config getConfig() {
        if (ENV.equalsIgnoreCase("prod")) {
            return ConfigFactory.load("application-prod.conf");
        }

        return ConfigFactory.load("application-dev.conf");
    }
}
