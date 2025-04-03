package org.apache.hop.pipeline.transforms.ksaar;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import org.apache.hop.core.exception.HopException;
import org.json.JSONArray;
import org.json.JSONObject;

public class Ksaar {

  public static String getApplication(String token, String applicationName) throws HopException {
    try {
      URL url = new URL("https://api.ksaar.co/v1/applications");
      HttpURLConnection con = (HttpURLConnection) url.openConnection();
      con.setRequestMethod("GET");

      con.setRequestProperty("Authorization", "Basic " + token);
      con.setRequestProperty("Content-Type", "application/json");

      BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
      String inputLine;
      StringBuilder response = new StringBuilder();

      while ((inputLine = in.readLine()) != null) {
        response.append(inputLine);
      }
      in.close();
      con.disconnect();

      JSONObject jsonResponse = new JSONObject(response.toString());
      JSONArray results = jsonResponse.getJSONArray("results");

      for (int i = 0; i < results.length(); i++) {
        JSONObject application = results.getJSONObject(i);

        String name = application.getString("name");
        String id = application.getString("id");

        if (name.equals(applicationName)) return id;
      }

      return null;
    } catch (Exception e) {
      throw new HopException("Erreur lors de l'appel à l'API Ksaar", e);
    }
  }

  public static String getWorkflow(String token, String applicationId, String workflowName)
      throws HopException {
    try {
      URL url = new URL("https://api.ksaar.co/v1/applications/" + applicationId + "/workflows");
      HttpURLConnection con = (HttpURLConnection) url.openConnection();
      con.setRequestMethod("GET");

      con.setRequestProperty("Authorization", "Basic " + token);
      con.setRequestProperty("Content-Type", "application/json");

      BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
      String inputLine;
      StringBuilder response = new StringBuilder();

      while ((inputLine = in.readLine()) != null) {
        response.append(inputLine);
      }
      in.close();
      con.disconnect();

      JSONObject jsonResponse = new JSONObject(response.toString());
      JSONArray results = jsonResponse.getJSONArray("results");

      for (int i = 0; i < results.length(); i++) {
        JSONObject workflow = results.getJSONObject(i);

        String name = workflow.getString("name");
        String id = workflow.getString("id");

        if (name.equals(workflowName)) return id;
      }

      return null;
    } catch (Exception e) {
      throw new HopException("Erreur lors de l'appel à l'API Ksaar", e);
    }
  }

  public static JSONArray getRecords(String token, String workflowId) throws HopException {
    try {
      URL url = new URL("https://api.ksaar.co/v1/workflows/" + workflowId + "/records");
      HttpURLConnection con = (HttpURLConnection) url.openConnection();
      con.setRequestMethod("GET");

      con.setRequestProperty("Authorization", "Basic " + token);
      con.setRequestProperty("Content-Type", "application/json");

      BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
      String inputLine;
      StringBuilder response = new StringBuilder();

      while ((inputLine = in.readLine()) != null) {
        response.append(inputLine);
      }
      in.close();
      con.disconnect();

      JSONObject jsonResponse = new JSONObject(response.toString());
      JSONArray results = jsonResponse.getJSONArray("results");

      return results;
    } catch (Exception e) {
      throw new HopException("Erreur lors de l'appel à l'API Ksaar", e);
    }
  }

  public static JSONArray getFields(String token, String workflowId) throws HopException {
    try {
      URL url = new URL("https://api.ksaar.co/v1/workflows/" + workflowId + "/fields");
      HttpURLConnection con = (HttpURLConnection) url.openConnection();
      con.setRequestMethod("GET");

      con.setRequestProperty("Authorization", "Basic " + token);
      con.setRequestProperty("Content-Type", "application/json");

      BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
      String inputLine;
      StringBuilder response = new StringBuilder();

      while ((inputLine = in.readLine()) != null) {
        response.append(inputLine);
      }
      in.close();
      con.disconnect();

      JSONArray jsonResponse = new JSONArray(response.toString());
      System.out.println(jsonResponse.toString());

      return jsonResponse;
    } catch (Exception e) {
      throw new HopException("Erreur lors de l'appel à l'API Ksaar", e);
    }
  }

  public static boolean bulkCreate(String token, String jsonPayload) throws HopException {
    HttpURLConnection con = null;
    try {
      URL url = new URL("https://api.ksaar.co/v1/records/bulkCreate");
      con = (HttpURLConnection) url.openConnection();
      con.setRequestMethod("POST");
      con.setDoOutput(true);

      // Set request headers
      con.setRequestProperty("Authorization", "Basic " + token);
      con.setRequestProperty("Content-Type", "application/json");

      // Sending the payload to the API
      try (OutputStream os = con.getOutputStream();
          OutputStreamWriter osw = new OutputStreamWriter(os, StandardCharsets.UTF_8)) {
        osw.write(jsonPayload);
        osw.flush();
      }

      int responseCode = con.getResponseCode();
      System.out.println("Response Code: " + responseCode);

      con.disconnect();

      return true;
    } catch (IOException e) {
      throw new HopException("Erreur lors de l'appel à l'API Ksaar", e);
    }
  }

  public static boolean bulkUpdate(String token, String jsonPayload) throws HopException {
    HttpURLConnection con = null;
    try {
      URL url = new URL("https://api.ksaar.co/v1/records/bulkUpdate");
      con = (HttpURLConnection) url.openConnection();
      con.setRequestMethod("POST");
      con.setDoOutput(true);

      // Set request headers
      con.setRequestProperty("Authorization", "Basic " + token);
      con.setRequestProperty("Content-Type", "application/json");

      // Sending the payload to the API
      try (OutputStream os = con.getOutputStream();
          OutputStreamWriter osw = new OutputStreamWriter(os, StandardCharsets.UTF_8)) {
        osw.write(jsonPayload);
        osw.flush();
      }

      int responseCode = con.getResponseCode();
      System.out.println("Response Code: " + responseCode);

      con.disconnect();

      return true;
    } catch (IOException e) {
      throw new HopException("Erreur lors de l'appel à l'API Ksaar", e);
    }
  }

  public static boolean bulkDelete(String token, String jsonPayload) throws HopException {
    HttpURLConnection con = null;
    try {
      URL url = new URL("https://api.ksaar.co/v1/records/bulkDelete");
      con = (HttpURLConnection) url.openConnection();
      con.setRequestMethod("POST");
      con.setDoOutput(true);

      // Set request headers
      con.setRequestProperty("Authorization", "Basic " + token);
      con.setRequestProperty("Content-Type", "application/json");

      // Sending the payload to the API
      try (OutputStream os = con.getOutputStream();
          OutputStreamWriter osw = new OutputStreamWriter(os, StandardCharsets.UTF_8)) {
        osw.write(jsonPayload);
        osw.flush();
      }

      int responseCode = con.getResponseCode();
      System.out.println("Response Code: " + responseCode);

      con.disconnect();

      return true;
    } catch (IOException e) {
      throw new HopException("Erreur lors de l'appel à l'API Ksaar", e);
    }
  }
}
