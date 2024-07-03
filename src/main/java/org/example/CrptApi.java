package org.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class CrptApi {
  private final int requestLimit;
  private final Semaphore semaphore;
  private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
  private static final HttpClient httpClient = HttpClient.newHttpClient();
  private static final String apiUrl = "https://ismp.crpt.ru/api/v3/lk/documents/create";
  private static final ObjectMapper objectMapper = new ObjectMapper();
  private final long intervalInMillis;

  public CrptApi(TimeUnit timeUnit, int requestLimit) {
    this.requestLimit = requestLimit;
    this.semaphore = new Semaphore(requestLimit);
    this.intervalInMillis = timeUnit.toMillis(1);
  }

  public void startScheduler() {
    scheduler.scheduleAtFixedRate(
        this::resetRequestCount, intervalInMillis, intervalInMillis, TimeUnit.MILLISECONDS);
  }

  private void resetRequestCount() {
    semaphore.release(requestLimit - semaphore.availablePermits());
  }

  public void createDocument(Document document, String signature) throws InterruptedException {
    semaphore.tryAcquire(1, intervalInMillis, TimeUnit.MILLISECONDS);
    HttpRequest request = buildRequest(document, signature);
    try {
      HttpResponse<String> response =
          httpClient.send(request, HttpResponse.BodyHandlers.ofString());
      System.out.println("The request has been completed. Response code: " + response.statusCode());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private HttpRequest buildRequest(Document document, String signature) {
    try {
      String jsonDocument = objectMapper.writeValueAsString(document);
      return HttpRequest.newBuilder()
          .uri(URI.create(apiUrl))
          .header("Content-Type", "application/json")
          .POST(HttpRequest.BodyPublishers.ofString(jsonDocument))
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize document to JSON", e);
    }
  }

  public void shutdown() {
    scheduler.shutdown();
    try {
      scheduler.awaitTermination(intervalInMillis, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) {
    CrptApi api = new CrptApi(TimeUnit.MINUTES, 3);
    api.startScheduler();

    Document document =
        new Document(
            new Description("1234567890"),
            "12345",
            "pending",
            "LP_INTRODUCE_GOODS",
            true,
            "0987654321",
            "9876543210",
            "5432109876",
            "2021-07-05",
            "Food",
            List.of(
                new Product(
                    "cert123",
                    "2023-07-05",
                    "cert456",
                    "1234567890",
                    "9876543210",
                    "2023-07-05",
                    "code123",
                    "uit123",
                    "uitu123")),
            "2023-07-05",
            "54321");

    String signature = "fakeSignature";

    try {
      for (int i = 0; i < api.requestLimit + 2; i++) {
        api.createDocument(document, signature);
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    } finally {
      api.shutdown();
    }
  }

  public record Document(
      Description description,
      String doc_id,
      String doc_status,
      String doc_type,
      boolean importRequest,
      String owner_inn,
      String participant_inn,
      String producer_inn,
      String production_date,
      String production_type,
      List<Product> products,
      String reg_date,
      String reg_number) {}

  public record Description(String participantInn) {}

  public record Product(
      String certificate_document,
      String certificate_document_date,
      String certificate_document_number,
      String owner_inn,
      String producer_inn,
      String production_date,
      String tnved_code,
      String uit_code,
      String uitu_code) {}
}
