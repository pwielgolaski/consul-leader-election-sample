package me.wielgolaski;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.ConsulRawClient;
import com.ecwid.consul.v1.QueryParams;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.agent.model.NewService;
import com.ecwid.consul.v1.kv.model.GetValue;
import com.ecwid.consul.v1.kv.model.PutParams;
import com.ecwid.consul.v1.session.model.NewSession;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class SampleService {

    private Logger logger = LoggerFactory.getLogger(SampleService.class);

    private static final int DEFAULT_CONNECTION_TIMEOUT = 10000; // 10 sec
    private static final int DEFAULT_READ_TIMEOUT = 10 * 60000; // 10 min (long wait)

    private final ConsulClient consul;
    private final String serviceName;
    private final String serviceId;
    private Optional<String> sessionId = Optional.empty();

    public SampleService(String serviceName) {
        RequestConfig requestConfig = RequestConfig.custom().
                setConnectTimeout(DEFAULT_CONNECTION_TIMEOUT).
                setConnectionRequestTimeout(DEFAULT_CONNECTION_TIMEOUT).
                setSocketTimeout(DEFAULT_READ_TIMEOUT).
                build();
        this.consul = new ConsulClient(new ConsulRawClient(HttpClients.custom()
                .setDefaultRequestConfig(requestConfig)
                .useSystemProperties()
                .build()));
        this.serviceName = serviceName;
        this.serviceId = serviceName + "-" + System.currentTimeMillis();
    }

    private void register() {
        NewService.Check ttlCheck = new NewService.Check();
        ttlCheck.setTtl("20s");
        ttlCheck.setDeregisterCriticalServiceAfter("1m");

        NewService service = new NewService();
        service.setId(serviceId);
        service.setName(serviceName);
        service.setCheck(ttlCheck);

        consul.agentServiceRegister(service);
    }

    private void runHeartBeat(int interval) {
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(
                () -> {
                    consul.agentCheckPass(getServiceCheck());
                    logger.info("Mark as passing {}", getServiceCheck());
                },
                0,
                interval, TimeUnit.SECONDS);
    }

    private String getServiceCheck() {
        return "service:" + serviceId;
    }

    private String getServiceLeaderKey() {
        return "service/" + serviceName + "/leader";
    }

    private String getSessionId() {
        if (!sessionId.isPresent()) {
            NewSession session = new NewSession();
            session.setChecks(Collections.singletonList(getServiceCheck()));
            sessionId = Optional.of(consul.sessionCreate(session, QueryParams.DEFAULT).getValue());
        }
        return sessionId.get();
    }

    private Long electLeaderIfNeeded(Response<GetValue> leaderResponse) {
        logger.info("leader key response {}", leaderResponse);
        if (leaderResponse.getValue() == null ||
                leaderResponse.getValue().getSession() == null ||
                "null".equals(leaderResponse.getValue().getSession())) {
            PutParams params = new PutParams();
            params.setAcquireSession(getSessionId());

            Response<Boolean> response = consul.setKVValue(getServiceLeaderKey(), serviceId, params);
            if (response.getValue()) {
                logger.warn("I'm leader {}", serviceId);
            } else {
                logger.info("I'm not leader {}", serviceId);
            }
        } else {
            logger.info("Leader session: {}, value: {}", leaderResponse.getValue().getSession(), leaderResponse.getValue().getDecodedValue());
        }
        return leaderResponse.getConsulIndex();
    }

    private void monitorLeader() {
        Response<GetValue> leaderResponse = consul.getKVValue(getServiceLeaderKey());
        while (true) {
            Long consulIndex = electLeaderIfNeeded(leaderResponse);
            logger.info("Monitor key {} with index {}", getServiceLeaderKey(), consulIndex);
            leaderResponse = consul.getKVValue(getServiceLeaderKey(), new QueryParams(-1, consulIndex));
        }
    }

    public static void main(String[] args) {
        SampleService service = new SampleService("myService");
        service.register();
        service.runHeartBeat(10);

        service.monitorLeader();
    }
}
