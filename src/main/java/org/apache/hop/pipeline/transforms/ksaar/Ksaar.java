package org.apache.hop.pipeline.transforms.ksaar;


import java.io.BufferedReader;
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

  public static JSONArray getApplication(String token) throws HopException {
    try {
      URL url = new URL("https://api.ksaar.co/v1/applications");
      HttpURLConnection con = (HttpURLConnection) url.openConnection();
      con.setConnectTimeout(10000);
      con.setRequestMethod("GET");

      con.setRequestProperty("Authorization", "Basic " + token);
      con.setRequestProperty("Content-Type", "application/json");
      System.out.println(con.toString().toString());
      int responseCode = con.getResponseCode();

      switch (responseCode) {
        case HttpURLConnection.HTTP_OK:
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

        case HttpURLConnection.HTTP_UNAUTHORIZED:
          throw new HopException("Ksaar.Error.Token");

        default:
          throw new HopException("Erreur with ksaar to get application. " + responseCode);
      }
    } catch (Exception e) {
      throw new HopException(e.getMessage());
    }
  }

  public static JSONArray getWorkflows(String token, String applicationId) throws HopException {
    try {
      URL url = new URL("https://api.ksaar.co/v1/applications/" + applicationId + "/workflows");
      HttpURLConnection con = (HttpURLConnection) url.openConnection();
      con.setConnectTimeout(10000);
      con.setRequestMethod("GET");

      con.setRequestProperty("Authorization", "Basic " + token);
      con.setRequestProperty("Content-Type", "application/json");

      int responseCode = con.getResponseCode();

      switch (responseCode) {
        case HttpURLConnection.HTTP_OK:
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

        default:
          throw new HopException(
              "Erreur with ksaar to get workflows of application: "
                  + applicationId
                  + ". "
                  + responseCode);
      }
    } catch (Exception e) {
      throw new HopException(e.getMessage());
    }
  }

  public static JSONArray getRecords(String token, String workflowId) throws HopException {
    try {
      URL url = new URL("https://api.ksaar.co/v1/workflows/" + workflowId + "/records");
      HttpURLConnection con = (HttpURLConnection) url.openConnection();
      con.setConnectTimeout(10000);
      con.setRequestMethod("GET");

      con.setRequestProperty("Authorization", "Basic " + token);
      con.setRequestProperty("Content-Type", "application/json");

      int responseCode = con.getResponseCode();

      switch (responseCode) {
        case HttpURLConnection.HTTP_OK:
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

        default:
          throw new HopException(
              "Erreur with ksaar to get records of workflow: " + workflowId + ". " + responseCode);
      }
    } catch (Exception e) {
      throw new HopException(e.getMessage());
    }
  }

  public static JSONArray getFields(String token, String workflowId) throws HopException {
    try {
      URL url = new URL("https://api.ksaar.co/v1/workflows/" + workflowId + "/fields");
      HttpURLConnection con = (HttpURLConnection) url.openConnection();
      con.setConnectTimeout(10000);
      con.setRequestMethod("GET");

      con.setRequestProperty("Authorization", "Basic " + token);
      con.setRequestProperty("Content-Type", "application/json");

      int responseCode = con.getResponseCode();

      switch (responseCode) {
        case HttpURLConnection.HTTP_OK:
          BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
          String inputLine;
          StringBuilder response = new StringBuilder();

          while ((inputLine = in.readLine()) != null) {
            response.append(inputLine);
          }
          in.close();
          con.disconnect();

          JSONArray jsonResponse = new JSONArray(response.toString());

          return jsonResponse;

        default:
          throw new HopException(
              "Erreur with ksaar to get fields of workflow: " + workflowId + ". " + responseCode);
      }
    } catch (Exception e) {
      throw new HopException(e.getMessage());
    }
  }

  public static JSONArray getUsers(String token, String applicationId) throws HopException {
    try {
      URL url = new URL("https://api.ksaar.co/v1/applications/" + applicationId + "/users");
      HttpURLConnection con = (HttpURLConnection) url.openConnection();
      con.setConnectTimeout(10000);
      con.setRequestMethod("GET");

      con.setRequestProperty("Authorization", "Basic " + token);
      con.setRequestProperty("Content-Type", "application/json");

      int responseCode = con.getResponseCode();

      switch (responseCode) {
        case HttpURLConnection.HTTP_OK:
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

        default:
          throw new HopException(
              "Erreur with ksaar to get users of application: "
                  + applicationId
                  + ". "
                  + responseCode);
      }
    } catch (Exception e) {
      throw new HopException(e.getMessage());
    }
  }

  public static boolean bulkCreate(String token, String jsonPayload) throws HopException {
    HttpURLConnection con = null;
    try {
      URL url = new URL("https://api.ksaar.co/v1/records/bulkCreate");
      con = (HttpURLConnection) url.openConnection();
      con.setRequestMethod("POST");
      con.setDoOutput(true);
      System.out.println(token);
      con.setRequestProperty("Authorization", "Basic " + token);
      con.setRequestProperty("Content-Type", "application/json");

      try (OutputStream os = con.getOutputStream();
          OutputStreamWriter osw = new OutputStreamWriter(os, StandardCharsets.UTF_8)) {
        osw.write(jsonPayload);
        osw.flush();
      }

      int responseCode = con.getResponseCode();

      switch (responseCode) {
        case HttpURLConnection.HTTP_CREATED:
          con.disconnect();
          return true;

        default:
          throw new HopException("Erreur with ksaar to bulkCreate. " + responseCode);
      }
    } catch (Exception e) {
      throw new HopException(e.getMessage());
    }
  }

  public static boolean bulkUpdate(String token, String jsonPayload) throws HopException {
    HttpURLConnection con = null;
    try {
      URL url = new URL("https://api.ksaar.co/v1/records/bulkUpdate");
      con = (HttpURLConnection) url.openConnection();
      con.setRequestMethod("POST");
      con.setDoOutput(true);

      con.setRequestProperty("Authorization", "Basic " + token);
      con.setRequestProperty("Content-Type", "application/json");

      try (OutputStream os = con.getOutputStream();
          OutputStreamWriter osw = new OutputStreamWriter(os, StandardCharsets.UTF_8)) {
        osw.write(jsonPayload);
        osw.flush();
      }

      int responseCode = con.getResponseCode();

      switch (responseCode) {
        case HttpURLConnection.HTTP_OK:
          con.disconnect();
          return true;

        default:
          throw new HopException("Erreur with ksaar to bulkUpdate. " + responseCode);
      }
    } catch (Exception e) {
      throw new HopException(e.getMessage());
    }
  }

  public static boolean bulkDelete(String token, String jsonPayload) throws HopException {
    HttpURLConnection con = null;
    try {
      URL url = new URL("https://api.ksaar.co/v1/records/bulkDelete");
      con = (HttpURLConnection) url.openConnection();
      con.setRequestMethod("POST");
      con.setDoOutput(true);

      con.setRequestProperty("Authorization", "Basic " + token);
      con.setRequestProperty("Content-Type", "application/json");

      try (OutputStream os = con.getOutputStream();
          OutputStreamWriter osw = new OutputStreamWriter(os, StandardCharsets.UTF_8)) {
        osw.write(jsonPayload);
        osw.flush();
      }

      int responseCode = con.getResponseCode();

      switch (responseCode) {
        case HttpURLConnection.HTTP_NO_CONTENT:
          con.disconnect();
          return true;

        default:
          throw new HopException("Erreur with ksaar to bulkDelete. " + responseCode);
      }
    } catch (Exception e) {
      throw new HopException(e.getMessage());
    }
  }
}
