package com.datasift.dropwizard.health;

import com.yammer.metrics.core.HealthCheck;

import java.net.Socket;

/**
 * A {@link HealthCheck} for a remote Graphite instance.
 */
public class GraphiteHealthCheck extends SocketHealthCheck {

    /**
     * Create a new {@link HealthCheck} instance for a remote Graphite server.
     *
     * @param host the hostname of the remote socket to connect to
     * @param port the port of the remote socket to connect to
     * @param name the name of this {@link GraphiteHealthCheck}
     */
    public GraphiteHealthCheck(final String host,
                               final int port,
                               final String name) {
        super(host, port, name + "-graphite");
    }

    @Override
    protected Result check(final Socket socket) {
        // TODO: find a way to verify the socket is connected to a Graphite server
        return Result.healthy();
    }
}
