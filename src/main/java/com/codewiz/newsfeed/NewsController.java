package com.codewiz.newsfeed;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Gatherers;

@Controller
public class NewsController {

    @GetMapping(value = "/news", produces = "text/event-stream")
    public Flux<ServerSentEvent<List<Article>>> news() throws IOException {
        Resource resource = new ClassPathResource("news.json");
        Path path = resource.getFile().toPath();
        String json = Files.readString(path);
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        News news = objectMapper.readValue(json, News.class);

        return Flux.fromIterable(news.articles())
                .window(3)
                .delayElements(Duration.ofSeconds(1))
                .flatMap(Flux::collectList)
                .map(articles -> ServerSentEvent.<List<Article>>builder()
                        .event("news")
                        .data(articles)
                        .build());

    }

    private final ExecutorService nonBlockingService = Executors.newCachedThreadPool();

    @GetMapping(value = "/news-emitter")
    public SseEmitter newsEmitter() throws IOException {
        SseEmitter emitter = new SseEmitter();
        Resource resource = new ClassPathResource("news.json");
        Path path = resource.getFile().toPath();
        String json = Files.readString(path);

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        News news = objectMapper.readValue(json, News.class);
        nonBlockingService.execute(() -> {
            try {
                news.articles().stream()
                        .gather(Gatherers.windowFixed(3))
                        .forEach(articles -> {
                            try {
                                emitter.send(articles);
                                Thread.sleep(1000);
                            } catch (Exception e) {
                                emitter.completeWithError(e);
                            }

                        });
                emitter.complete();
            } catch (Exception ex) {
                emitter.completeWithError(ex);
            }
        });
        return emitter;
    }

}
