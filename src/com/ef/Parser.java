package com.ef;

import java.sql.*;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Scanner;

import org.joda.time.DateTime;
import org.joda.time.Period;

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
      con = DriverManager.getConnection("jdbc:mysql://ipparser.c3fxdgnnonos.us-east-2.rds.amazonaws.com",
          "administrator", "abcdefgh");
      con.setCatalog("IPParser");
      blockedUpdate = con.prepareStatement("insert into blocked values(?,?)");
      logUpdate = con.prepareStatement("insert into log values(?,?,?,?)");
    } catch (ClassNotFoundException exc)
    {
      System.out.println(exc.getStackTrace());
    } catch (SQLException sqe)
    {
      System.out.println(sqe);
    }
  }

  public void blockedUpdate(String ip, int count)
  {
    try
    {
      blockedUpdate.setString(1, ip);
      blockedUpdate.setInt(2, count);
      blockedUpdate.executeUpdate();
    } catch (SQLException sqe)
    {
      System.out.println("insert into blocked failed");
      System.out.println(sqe.getStackTrace());
    }
  }

  public static void main(String[] args) throws Exception
  {
    Parser p = new Parser();
    int threshhold = 150;

    int duration = 1; // might be 24 hour or 1 hour.

    HashMap<String, Integer> ipCounts = new HashMap<>();
    FileInputStream fin;
    try
    {

      fin = new FileInputStream("access.log");
    } catch (FileNotFoundException exc)
    {
      System.out.println("File not found");
      return;
    }

    Scanner sc = new Scanner(fin);
    while (sc.hasNextLine())
    {

      String s = sc.nextLine();
      String[] infos = s.split("\\|");
      String ip = infos[1];
      String[] times = infos[0].split(" ");
      DateTime dt = new DateTime(times[0] + "T" + times[1]);
      if (ipCounts.containsKey(ip))
        ipCounts.put(ip, ipCounts.get(ip) + 1);
      else
        ipCounts.put(ip, 1);

    }
    for (String ip0 : ipCounts.keySet())
    {
      if (ipCounts.get(ip0) > threshhold)
      {
        System.out.println(ip0 + " count " + ipCounts.get(ip0));
        // Statement stmt= con.createStatement();
        // stmt.executeUpdate("Insert into blocked values(10.0.0.0, 20)");
      }
    }

    DateTime dt = new DateTime("2012-01-01T00:00:00.000");
    DateTime dt2 = dt.plus(Period.hours(24));
    System.out.println(dt2);

    // Statement stmt= con.createStatement();
    // stmt.executeUpdate("Insert into blocked values('10.0.0.0', 20)");
    p.blockedUpdate("10.1.1.1", 30);
  }
}
