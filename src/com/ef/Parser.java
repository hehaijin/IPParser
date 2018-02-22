package com.ef;

import java.sql.*;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import org.joda.time.DateTime;
import org.joda.time.Period;

import com.sun.java.accessibility.AccessBridge;

public class Parser
{
  Connection con; // the database connection
  PreparedStatement blockedUpdate;
  PreparedStatement logUpdate;

  public Parser()
  {
    try
    {
      Class.forName("com.mysql.jdbc.Driver");
      con = DriverManager.getConnection(
          "jdbc:mysql://ipparser.c3fxdgnnonos.us-east-2.rds.amazonaws.com?rewriteBatchedStatements=true",
          "administrator", "abcdefgh");
      con.setCatalog("IPParser");
      blockedUpdate = con
          .prepareStatement("insert into blocked (ip, start, duration, count, comment) values(?,?,?,?,?)");
      logUpdate = con.prepareStatement("insert into log (ip,time,request, status,agent) values (?,?,?,?,?)");
    } catch (ClassNotFoundException exc)
    {
      System.out.println(exc.getStackTrace());
    } catch (SQLException sqe)
    {
      System.out.println("something went wrong when initiating the database connection");
      return;
    }
  }

  private void blockedUpdate(String ip, Timestamp ts, String duration, int count, String comment)
  {
    try
    {
      blockedUpdate.setString(1, ip);
      blockedUpdate.setTimestamp(2, ts);
      blockedUpdate.setString(3, duration);
      blockedUpdate.setInt(4, count);
      blockedUpdate.setString(5, comment);
      blockedUpdate.executeUpdate();
    } catch (SQLException sqe)
    {
      System.out.println("insert into blocked failed");
      System.out.println(sqe.getMessage());
    }
  }

  private static Map<String, String> parseParameters(String[] args) throws Exception
  {
    Map<String, String> paras = new HashMap<>();
    for (String arg : args)
    {
      if (arg.startsWith("--accesslog"))
        paras.put("accesslog", arg.split("=")[1]);
      if (arg.startsWith("--startDate"))
        paras.put("startDate", arg.split("=")[1].replace('.', 'T'));
      if (arg.startsWith("--duration"))
      {
        String duration = arg.split("=")[1];
        if (!duration.equals("hourly") && !duration.equals("daily"))
          throw new Exception("duration must be daily or hourly");
        paras.put("duration", duration);
      }
      if (arg.startsWith("--threshold"))
        paras.put("threshold", arg.split("=")[1]);
    }
    if (!paras.containsKey("accesslog"))
      paras.put("accesslog", "access.log");
    if (!paras.containsKey("startDate"))
      throw new Exception("startDate must be specified");
    if (!paras.containsKey("duration"))
      throw new Exception("duration must be specified");
    if (!paras.containsKey("threshold"))
      throw new Exception("threshold must be specified");
    return paras;
  }

  private List<AccessRecord> readFile(FileInputStream fin)
  {
    List<AccessRecord> logs = new ArrayList<>();
    try (Scanner sc = new Scanner(fin))
    {
      while (sc.hasNextLine())
        logs.add(AccessRecord.parse(sc.nextLine()));
    }
    return logs;
  }

  private void insertAlltoLog(List<AccessRecord> records)
  {
    try
    {
      con.setAutoCommit(false);
      int i = 0;

      for (AccessRecord ar : records)
      {
        logUpdate.setString(1, ar.ip);
        DateTime dt = new DateTime(ar.time);
        logUpdate.setTimestamp(2, new Timestamp(dt.getMillis()));
        logUpdate.setString(3, ar.request);
        logUpdate.setInt(4, ar.status);
        logUpdate.setString(5, ar.agent);
        logUpdate.addBatch();
        i++;

        if (i % 10000 == 0 || i == records.size())
        {
          try
          {
            System.out.println(i);
            // con.setAutoCommit(false);
            logUpdate.executeLargeBatch(); // Execute every 1000 items.
            con.commit();
          } catch (Exception e)
          {
            System.out.println(e.getMessage());
          }
          System.out.println("batch return");
        }

      }
      this.con.setAutoCommit(true);

    } catch (Exception e)
    {
      System.out.println(e.getMessage());
    }
  }

  static class AccessRecord
  {
    String ip;
    String request;
    String time;
    int status;
    String agent;

    public AccessRecord(String ip, String time, String request, int status, String agent)
    {
      this.ip = ip;
      this.request = request;
      this.time = time;
      this.status = status;
      this.agent = agent;
    }

    static public AccessRecord parse(String input)
    {
      String[] infos = input.split("\\|");
      String ip = infos[1];
      String request = infos[2];
      int status = Integer.parseInt(infos[3]);
      String[] accesstime = infos[0].split(" ");
      String time = accesstime[0] + "T" + accesstime[1];
      // System.out.println(time);
      String agent = infos[4];
      return new AccessRecord(ip, time, request, status, agent);
    }
  }

  public static void main(String[] args) throws Exception
  {
    // get parameters
    Map<String, String> paras;
    try
    {
      paras = parseParameters(args);
    } catch (Exception e)
    {
      System.out.println(e);
      return;
    }

    DateTime start = new DateTime(paras.get("startDate"));
    DateTime end;
    if (paras.get("duration") == "hourly")
      end = start.plusHours(1);
    else
      end = start.plusDays(1);
    int threshold = Integer.parseInt(paras.get("threshold"));
    Parser p = new Parser();
    HashMap<String, Integer> ipCounts = new HashMap<>();
    List<AccessRecord> records;

    try (FileInputStream fin = new FileInputStream(paras.get("accesslog")))
    {
      records = p.readFile(fin);
    } catch (FileNotFoundException exc)
    {
      System.out.println("File not found");
      return;
    }
    // dump to database the whole log
    // p.insertAlltoLog(records);

    // filter and dump to database the blocked ip.
    for (AccessRecord ar : records)
    {
      DateTime dt = new DateTime(ar.time);
      if (dt.isAfter(start) && dt.isBefore(end))
      {
        if (ipCounts.containsKey(ar.ip))
          ipCounts.put(ar.ip, ipCounts.get(ar.ip) + 1);
        else
          ipCounts.put(ar.ip, 1);
      }
    }

    // p.con.setAutoCommit(false);
    for (String ip0 : ipCounts.keySet())
    {
      if (ipCounts.get(ip0) > threshold)
      {
        System.out.println(ip0 + " count " + ipCounts.get(ip0));
        String comment = "blocked because it exceeds the threshhold of " + threshold + " in the period of "
            + (paras.get("duration").equals("hourly") ? "one hour" : "one day") + " starting from " + start.toString();
        p.blockedUpdate(ip0, new Timestamp(start.getMillis()), paras.get("duration"), ipCounts.get(ip0), comment);
      }
    }

  }
}
