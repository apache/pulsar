/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.common.sasl;

import java.util.Date;
import java.util.Random;
import java.util.Set;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import lombok.CustomLog;

/**
 * TGT Refresh Thread. Copied from Apache ZooKeeper TGT refresh logic.
 */
@CustomLog
public class TGTRefreshThread extends Thread {

    private static final Random rng = new Random();

    private long lastLogin;
    private final JAASCredentialsContainer container;

    public long getLastLogin() {
        return lastLogin;
    }

    public void setLastLogin(long lastLogin) {
        this.lastLogin = lastLogin;
    }

    public TGTRefreshThread(JAASCredentialsContainer container) {
        this.container = container;
        // Initialize 'lastLogin' to do a login at first time
        this.lastLogin = System.currentTimeMillis() - MIN_TIME_BEFORE_RELOGIN;
        setDaemon(true);
        setName("pulsar-tgt-refresh-thread");
    } // Initialize 'lastLogin' to do a login at first time

    private synchronized KerberosTicket getTGT() {
        Set<KerberosTicket> tickets = container.getSubject().getPrivateCredentials(KerberosTicket.class);
        for (KerberosTicket ticket : tickets) {
            KerberosPrincipal server = ticket.getServer();
            if (server.getName().equals("krbtgt/" + server.getRealm() + "@" + server.getRealm())) {
                log.info().attr("clientPrincipal", ticket.getClient().getName())
                        .attr("serverPrincipal", ticket.getServer().getName())
                        .log("Found TGT ticket");
                return ticket;
            }
        }
        return null;
    }
    // LoginThread will sleep until 80% of time from last refresh to
    // ticket's expiry has been reached, at which time it will wake
    // and try to renew the ticket.
    private static final float TICKET_RENEW_WINDOW = 0.80f;
    /**
     * Percentage of random jitter added to the renewal time.
     */
    private static final float TICKET_RENEW_JITTER = 0.05f;
    // Regardless of TICKET_RENEW_WINDOW setting above and the ticket expiry time,
    // thread will not sleep between refresh attempts any less than 1 minute (60*1000 milliseconds = 1 minute).
    // Change the '1' to e.g. 5, to change this to 5 minutes.
    private static final long MIN_TIME_BEFORE_RELOGIN = 1 * 60 * 1000L;

    private long getRefreshTime(KerberosTicket tgt) {
        long start = tgt.getStartTime().getTime();
        long expires = tgt.getEndTime().getTime();
        log.info().attr("startTime", tgt.getStartTime().toString())
                .attr("endTime", tgt.getEndTime().toString()).log("TGT validity period");
        long proposedRefresh = start
            + (long) ((expires - start) * (TICKET_RENEW_WINDOW + (TICKET_RENEW_JITTER * rng.nextDouble())));
        if (proposedRefresh > expires) {
            // proposedRefresh is too far in the future: it's after ticket expires: simply return now.
            return System.currentTimeMillis();
        } else {
            return proposedRefresh;
        }
    }

    @Override
    public void run() {
        log.info("TGT refresh thread started.");
        while (!Thread.currentThread().isInterrupted()) {
            // renewal thread's main loop. if it exits from here, thread will exit.
            KerberosTicket tgt = getTGT();
            long now = System.currentTimeMillis();
            long nextRefresh;
            Date nextRefreshDate;
            if (tgt == null) {
                nextRefresh = now + MIN_TIME_BEFORE_RELOGIN;
                nextRefreshDate = new Date(nextRefresh);
                log.warn().attr("nextRetry", nextRefreshDate).log("No TGT found: will try again");
            } else {
                nextRefresh = getRefreshTime(tgt);
                long expiry = tgt.getEndTime().getTime();
                Date expiryDate = new Date(expiry);
                if ((container.isUsingTicketCache()) && (tgt.getEndTime().equals(tgt.getRenewTill()))) {
                    log.error()
                        .attr("expiryDate", expiryDate)
                        .attr("principal", container.getPrincipal())
                        .log("The TGT cannot be renewed beyond the next expiry date."
                                + " This process will not be able to authenticate new SASL"
                                + " connections after that time. Ask your system administrator"
                                + " to either increase the 'renew until' time by doing"
                                + " 'modprinc -maxrenewlife' within kadmin, or instead,"
                                + " to generate a keytab. Exiting refresh thread now.");
                    return;
                }
                // determine how long to sleep from looking at ticket's expiry.
                // We should not allow the ticket to expire, but we should take into consideration
                // MIN_TIME_BEFORE_RELOGIN. Will not sleep less than MIN_TIME_BEFORE_RELOGIN, unless doing so
                // would cause ticket expiration.
                if ((nextRefresh > expiry) || ((now + MIN_TIME_BEFORE_RELOGIN) > expiry)) {
                    // expiry is before next scheduled refresh).
                    nextRefresh = now;
                } else {
                    if (nextRefresh < (now + MIN_TIME_BEFORE_RELOGIN)) {
                        // next scheduled refresh is sooner than (now + MIN_TIME_BEFORE_LOGIN).
                        Date until = new Date(nextRefresh);
                        Date newuntil = new Date(now + MIN_TIME_BEFORE_RELOGIN);
                        log.warn()
                            .attr("from", until)
                            .attr("to", newuntil)
                            .attr("minIntervalSeconds", MIN_TIME_BEFORE_RELOGIN / 1000)
                            .log("TGT refresh thread time adjusted since the former is sooner"
                                    + " than the minimum refresh interval from now.");
                    }
                    nextRefresh = Math.max(nextRefresh, now + MIN_TIME_BEFORE_RELOGIN);
                }
                nextRefreshDate = new Date(nextRefresh);
                if (nextRefresh > expiry) {
                    log.error()
                        .attr("nextRefresh", nextRefreshDate)
                        .attr("expiry", expiryDate)
                        .log("next refresh is later than expiry. This may indicate a clock"
                                + " skew problem. Check that this host and the KDC's hosts'"
                                + " clocks are in sync. Exiting refresh thread.");
                    return;
                }
            }
            if (now == nextRefresh) {
                log.info("refreshing now because expiry is before next scheduled refresh time.");
            } else if (now < nextRefresh) {
                Date until = new Date(nextRefresh);
                log.info().attr("until", until.toString()).log("TGT refresh sleeping");
                try {
                    Thread.sleep(nextRefresh - now);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    log.warn("TGT renewal thread has been interrupted and will exit.");
                    break;
                }
            } else {
                log.error()
                    .attr("nextRefresh", nextRefreshDate)
                    .log("nextRefresh is in the past: exiting refresh thread. Check clock"
                            + " sync between this host and KDC - (KDC's clock is likely ahead"
                            + " of this host). Manual intervention will be required for this"
                            + " client to successfully authenticate.");
                break;
            }
            if (container.isUsingTicketCache()) {
                String cmd = container.getConfiguration().getOrDefault(SaslConstants.KINIT_COMMAND,
                    SaslConstants.KINIT_COMMAND_DEFAULT);
                String kinitArgs = "-R";
                int retry = 1;
                while (retry >= 0) {
                    try {
                        log.info().attr("cmd", cmd).attr("args", kinitArgs).log("running ticket cache refresh command");

                        ProcessBuilder processBuilder = new ProcessBuilder();
                        processBuilder.command("bash", "-c", cmd, kinitArgs);
                        break;
                    } catch (Exception e) {
                        if (retry > 0) {
                            --retry;
                            // sleep for 10 seconds
                            try {
                                Thread.sleep(10 * 1000);
                            } catch (InterruptedException ie) {
                                Thread.currentThread().interrupt();
                                log.error("Interrupted while renewing TGT, exiting Login thread");
                                return;
                            }
                        } else {
                            log.warn()
                                .attr("cmd", cmd)
                                .attr("args", kinitArgs)
                                .exception(e)
                                .log("Could not renew TGT due to problem running shell"
                                        + " command. Exiting refresh thread.");
                            return;
                        }
                    }
                }
            }
            try {
                int retry = 1;
                while (retry >= 0) {
                    try {
                        reLogin();
                        break;
                    } catch (LoginException le) {
                        if (retry > 0) {
                            --retry;
                            // sleep for 10 seconds.
                            try {
                                Thread.sleep(10 * 1000);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                log.error().exception(le).log("Interrupted during login retry after LoginException");
                                throw le;
                            }
                        } else {
                            log.error()
                            .attr("principal", container.getPrincipal())
                            .exception(le)
                            .log("Could not refresh TGT for principal");
                        }
                    }
                }
            } catch (LoginException le) {
                log.error().exception(le).log("Failed to refresh TGT: refresh thread exiting now.");
                break;
            }
        }
    }

    /**
     * Re-login a principal. This method assumes that {@link #login(String)} has happened already.
     * c.f. HADOOP-6559
     * @throws LoginException on a failure
     */
    private synchronized void reLogin() throws LoginException {
        LoginContext login = container.getLoginContext();
        if (login == null) {
            throw new LoginException("login must be done first");
        }
        if (!hasSufficientTimeElapsed()) {
            return;
        }
        log.info().attr("principal", container.getPrincipal()).log("Initiating logout");
        synchronized (this) {
            //clear up the kerberos state. But the tokens are not cleared! As per
            //the Java kerberos login module code, only the kerberos credentials
            //are cleared
            login.logout();
            //login and also update the subject field of this instance to
            //have the new credentials (pass it to the LoginContext constructor)
            login = new LoginContext(container.getLoginContextName(), container.getSubject());
            log.info().attr("principal", container.getPrincipal()).log("Initiating re-login");
            login.login();
            container.setLoginContext(login);
        }
    }

    private boolean hasSufficientTimeElapsed() {
        long now = System.currentTimeMillis();
        if (now - getLastLogin() < MIN_TIME_BEFORE_RELOGIN) {
            log.warn()
                .attr("minIntervalSeconds", MIN_TIME_BEFORE_RELOGIN / 1000)
                .log("Not attempting to re-login since the last re-login was"
                        + " attempted less than the minimum interval.");
            return false;
        }
        // register most recent relogin attempt
        setLastLogin(now);
        return true;
    }

}
