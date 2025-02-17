/*
 * Title:        CloudSim Toolkit
 * Description:  CloudSim (Cloud Simulation) Toolkit for Modeling and Simulation
 *               of Clouds
 * Licence:      GPL - http://www.gnu.org/copyleft/gpl.html
 *
 * Copyright (c) 2009, The University of Melbourne, Australia
 */

import Broker.MyBroker;
import MyDatacenter.MyDatacenter;
import org.cloudbus.cloudsim.Datacenter;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.core.CloudSim;
import org.jetbrains.annotations.NotNull;
import scheduller.Scheduler;
import workflow.Workflow;
import workflow.WorkflowPasser;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Objects;

/**
 * A simple example showing how to create a data center with one host and run one cloudlet on it.
 */
public class Simulator {
  private static final PrintStream standardOut = System.out;

  /**
   * Creates main() to run this example.
   *
   * @param args the args
   */
  public static void main(String[] args) throws IOException {


    List<String> workflowsPath = findDaxOn("./resources/workflow-data");

    int[][] timeTable = new int[workflowsPath.size()][Scheduler.SchedulingAlgorithm.values().length];
    int i = 0, j = 0;


    for (String workflowFilePath : workflowsPath) {
      for (Scheduler.SchedulingAlgorithm schedulingAlgorithm : Scheduler.SchedulingAlgorithm.values()) {
        timeTable[i][j++] = simulate(workflowFilePath, schedulingAlgorithm, "./resources/logs/", "./resources/cloudlets/");
      }
      i++;
      j = 0;
    }

    printTableInfo(workflowsPath, timeTable);
  }

  @NotNull
  private static List<String> findDaxOn(String folderPath) throws IOException {
    return Files.walk(Paths.get(folderPath))
      .filter((Files::isRegularFile))
      .map(Path::toString)
      .toList();
  }

  private static void printTableInfo(List<String> workflowsPath, int[][] timeTable) {

    int i = 0, j = 0;

    System.out.println("\n");
    System.out.printf("%-30s", "workflow/algorithm");
    Arrays.stream(Scheduler.SchedulingAlgorithm.values()).map(t -> t.name().substring(t.name().lastIndexOf("_") + 1)).forEach(t -> System.out.printf("\t%-10s", t));
    System.out.println("\n================================================================================");

    for (String workflowFilePath : workflowsPath) {
      System.out.printf("%-23s|%-6s", workflowFilePath.substring(workflowFilePath.lastIndexOf("\\") + 1), "");
      for (Scheduler.SchedulingAlgorithm ignored : Scheduler.SchedulingAlgorithm.values()) {
        System.out.printf("\t%-10s", timeTable[i][j++]);
      }
      i++;
      j = 0;
      System.out.println();
    }

    System.out.println("""
                  
      ================================================================================
                      Ferdowsi University Of Mashhad (FUM)
                                            Cloud Computing Course
                                            Dr.Saeed Abrishami
                                            Mohsen Gholami Golkhatmi | iMohsen02
                  
                      Workflows Scheduling Project Simulation (Cloudsim) - 2025
      ================================================================================

      """);
  }

  private static int simulate(String workflowFilePath, Scheduler.SchedulingAlgorithm schedulingAlgorithm, String brokerLogFolderPath, String cloudletLogFolderPath) throws FileNotFoundException {

    System.out.println("Workflow: " + workflowFilePath.substring(workflowFilePath.lastIndexOf("\\") + 1) + "\tAlgorithm: " + schedulingAlgorithm.name() + "...");

    // set different output stream for broker log and cloudlet
    String workflowFile = workflowFilePath.substring(workflowFilePath.lastIndexOf("\\") + 1, workflowFilePath.lastIndexOf("."));
    System.setOut(new PrintStream(new FileOutputStream(cloudletLogFolderPath + workflowFile + "-" + schedulingAlgorithm.name() + ".log")));
    Log.setOutput(new FileOutputStream(brokerLogFolderPath + workflowFile + "-" + schedulingAlgorithm.name() + ".log"));

    Log.printLine("Starting cloud simulation...");

    try {
      // First step: Initialize the CloudSim package. It should be called before creating any entities.
      int num_user = 1;                           // number of cloud users
      Calendar calendar = Calendar.getInstance(); // Calendar whose fields have been initialized with the current date and time.
      boolean trace_flag = false;                 // trace events

      CloudSim.init(num_user, calendar, trace_flag);

      // Second step: Create Datacenters
      // Datacenters are the resource providers in CloudSim. We need at
      // list one of them to run a CloudSim simulation
      Datacenter datacenter = MyDatacenter.createDatacenter("My_Datacenter");

      // Third step: Create Broker
      WorkflowPasser workflowPasser = new WorkflowPasser();
      Workflow workflow = workflowPasser.parse(workflowFilePath);
      workflow.preprocess(schedulingAlgorithm);

      DatacenterBroker broker = MyBroker.createMyBroker("My_Broker", workflow, schedulingAlgorithm);


      // Sixth step: Starts the simulation
      CloudSim.startSimulation();

      // Seventh step: End the simulation
      CloudSim.stopSimulation();

      Log.printLine("Cloud simulation finished!");

      // Eighth step: Return execution time
      System.setOut(standardOut);
      return (int) ((MyBroker) Objects.requireNonNull(broker)).finishTime;
    } catch (Exception e) {
      e.printStackTrace();
      Log.printLine("Unwanted errors happen");
    }
    return -1;
  }
}