package com.random.test;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.Empty;
import com.random.test.MutinyDummyServiceGrpc.MutinyDummyServiceStub;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.smallrye.mutiny.Uni;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.eclipse.microprofile.context.spi.ContextManagerProvider;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class DummyClientTest {
    private static final int port = 50051;
    private static final int parallel = 32;
    private static ScheduledExecutorService serverExecutor;
    private static ExecutorService clientExecutor;
    private static Server server;
    private static ManagedChannel channel;

    @BeforeAll
    static void beforeAll() {
        ContextManagerProvider.instance();
        serverExecutor = Executors.newSingleThreadScheduledExecutor();
        clientExecutor = Executors.newFixedThreadPool(parallel);
        server = ServerBuilder.forPort(port).addService(new DummyServiceImpl()).build();
        channel = ManagedChannelBuilder.forAddress("0.0.0.0", port).usePlaintext().build();
    }

    @AfterAll
    static void afterAll() {
        channel.shutdownNow();
        server.shutdown();
        serverExecutor.shutdownNow();
        clientExecutor.shutdownNow();
    }

    @Test
    @Timeout(value = 30)
    public void client_applies_retry_policy_when_server_is_unavailable() {
        serverExecutor.schedule(DummyClientTest::startServer, 3000, TimeUnit.MILLISECONDS);

        List<Future<String>> futures = new ArrayList<>(parallel);
        for (int i = 0; i < parallel; i++) {
            int j = i;
            futures.add(clientExecutor.submit(() -> {
                MutinyDummyServiceStub stub = MutinyDummyServiceGrpc.newMutinyStub(channel)
                    .withDeadlineAfter(10, TimeUnit.SECONDS);
                Response response = stub
                    .hello(Empty.getDefaultInstance())
                    .log("Request #" + j)
                    .stage(DummyClientTest::applyRetryPolicy)
                    .await().indefinitely();
                return response.getGreeting();
            }));
        }
        for (int i = 0; i < parallel; i++) {
            assertThat(futures.get(i))
                .describedAs("Request #" + i)
                .succeedsWithin(20, TimeUnit.SECONDS)
                .isEqualTo("Hello");
        }
    }

    private static void startServer() {
        try {
            server.start();
            System.err.println("Server started on port " + server.getPort());
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    private static Uni<Response> applyRetryPolicy(Uni<Response> uni) {
        return uni.onFailure(DummyClientTest::shouldRetry)
            .retry().withBackOff(Duration.ofSeconds(1), Duration.ofSeconds(2)).indefinitely();
    }

    private static boolean shouldRetry(Throwable throwable) {
        Status.Code code = Status.fromThrowable(throwable).getCode();
        return code == Status.Code.UNAVAILABLE;
    }
}

class DummyServiceImpl extends DummyServiceGrpc.DummyServiceImplBase {

    public DummyServiceImpl() {}

    @Override
    public void hello(Empty request, StreamObserver<Response> responseObserver) {
        responseObserver.onNext(Response.newBuilder().setGreeting("Hello").build());
        responseObserver.onCompleted();
    }
}
