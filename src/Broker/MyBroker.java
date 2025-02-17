package Broker;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;

import scheduller.Scheduler;
import workflow.Task;
import workflow.Workflow;

/**
 * The {@code MyBroker} class extends {@link DatacenterBroker} and manages the scheduling
 * and execution of cloudlets (tasks) in a CloudSim simulation.
 * It handles VM creation, cloudlet submission, and scheduling based on a specified algorithm.
 *
 * @author Mohsen Gholami | iMohsen02
 * @see Workflow
 * @see workflow.WorkflowPasser
 * @see scheduller.Scheduler
 * @see Broker.MyBroker
 * @see org.cloudbus.cloudsim.Cloudlet
 * @see MyDatacenter.MyDatacenter
 * @see Task
 * @since 2/7/2025
 */
public class MyBroker extends DatacenterBroker {

  /**
   * ID of the target datacenter
   */
  private final int datacenterId = 2;
  /**
   * List of cloudlets to be executed
   */
  private List<Task> cloudletList;
  /**
   * Scheduling algorithm for task execution
   */
  private final Scheduler scheduler;
  /**
   * Counter for VM IDs
   */
  private int vmId = 0;
  /**
   * List of VMs managed by this broker
   */
  private final List<Vm> vms;
  /**
   * Counter for executed cloudlets
   */
  private int executedCloudlets = 0;
  /**
   * Simulation finish time
   */
  public double finishTime = 0;

  /**
   * Creates a new {@code MyBroker} instance for managing workflow execution.
   *
   * @param name           The name of the broker.
   * @param workflow       The workflow containing tasks to be scheduled.
   * @param schedulingType The scheduling algorithm to be used.
   * @throws Exception if an error occurs during initialization.
   */
  public MyBroker(String name, Workflow workflow, Scheduler.SchedulingAlgorithm schedulingType) throws Exception {
    super(name);
    this.vms = new ArrayList<>();
    this.scheduler = new Scheduler(workflow, schedulingType);
  }

  /**
   * Factory method to create a {@code MyBroker} instance.
   *
   * @param name           The name of the broker.
   * @param workflow       The workflow to be scheduled.
   * @param schedulingType The scheduling algorithm.
   * @return A new {@code MyBroker} instance, or {@code null} if an error occurs.
   */
  public static DatacenterBroker createMyBroker(String name, Workflow workflow, Scheduler.SchedulingAlgorithm schedulingType) {
    try {
      return new MyBroker(name, workflow, schedulingType);
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }

  /**
   * Initializes the broker by creating VMs and retrieving the initial list of ready tasks.
   */
  @Override
  public void startEntity() {
    initVm(800, 1, 10_000, 512, 1_000, 4);
    initVm(1_200, 1, 10_000, 512, 1_000, 4);
    initVm(1_600, 1, 10_000, 512, 1_000, 4);

    cloudletList = scheduler.getReadyTasks();
  }

  /**
   * Processes simulation events.
   *
   * @param ev The simulation event to be processed.
   */
  @Override
  public void processEvent(SimEvent ev) {
    switch (ev.getTag()) {
      // VM Creation answer
      case CloudSimTags.VM_CREATE_ACK -> processVmCreate(ev);
      // A finished cloudlet returned
      case CloudSimTags.CLOUDLET_RETURN -> processCloudletReturn(ev);
      // if the simulation finishes
      case CloudSimTags.END_OF_SIMULATION -> shutdownEntity();
      // Schedule next cycle
      case CloudSimTags.NextCycle -> notifyScheduler();
      // other unknown tags are processed by this method
      default -> processOtherEvent(ev);
    }
  }


  /**
   * Notifies the scheduler to assign tasks to idle VMs and schedules the next cycle if tasks remain.
   */
  private void notifyScheduler() {
    scheduler.Schedule(getId());
    Log.printLine(CloudSim.clock() + ": scheduling => " + cloudletList.stream().map(Task::getDaxId).collect(Collectors.joining(", ")));

    List<Vm> idleVms = getIdleVms();

    Log.print(CloudSim.clock() + ": idle vms => ");
    idleVms.forEach(v -> Log.print(v.getId() + "\t"));
    Log.printLine();

    submitCloudletOnVms(idleVms);

    if (scheduler.isTaskRemains()) schedule(getId(), 600, CloudSimTags.NextCycle);
  }

  /**
   * Submits cloudlets to available VMs for execution.
   *
   * @param idleVms List of idle VMs that can execute tasks.
   */
  private void submitCloudletOnVms(List<Vm> idleVms) {
    for (Vm idleVm : idleVms) {
      if (cloudletList.isEmpty()) break;

      Cloudlet nextCloudlet = cloudletList.remove(0);
      nextCloudlet.setVmId(idleVm.getId());
      sendNow(datacenterId, CloudSimTags.CLOUDLET_SUBMIT, nextCloudlet);

      Log.printLine(CloudSim.clock() + ": cloudlet #" + nextCloudlet.getCloudletId() + " will execute on vm #" + nextCloudlet.getVmId());
    }
  }

  /**
   * Retrieves a list of idle VMs sorted by MIPS in descending order.
   *
   * @return A list of idle VMs available for task execution.
   */
  private List<Vm> getIdleVms() {
    return vms.stream()
      .filter(v -> v.getCloudletScheduler().runningCloudlets() == 0)
      .sorted(Comparator.comparingDouble(Vm::getMips).reversed())
      .toList();
  }

  /**
   * Processes the completion of a cloudlet.
   *
   * @param ev The simulation event containing the completed cloudlet.
   */
  @Override
  protected void processCloudletReturn(SimEvent ev) {
    Cloudlet cloudlet = (Cloudlet) ev.getData();
    Task task = (Task) cloudlet;

    scheduler.notifyExecuted(task);
    executedCloudlets++;

    Log.printLine(CloudSim.clock() + ": " + getName() + ": Cloudlet " + cloudlet.getCloudletId() +
      " is received (start: " + cloudlet.getExecStartTime() + ", end: " + cloudlet.getFinishTime() +
      ", length: " + cloudlet.getCloudletLength() + "). Executed on VM #" + cloudlet.getVmId() +
      " - " + executedCloudlets + " cloudlets executed.");
  }

  /**
   * Handles the response from the datacenter regarding VM creation.
   * Also call {@code NextCycle} event when last vm created.
   *
   * @param ev The simulation event containing VM creation details.
   */
  protected void processVmCreate(SimEvent ev) {
    int[] data = (int[]) ev.getData();
    int datacenterId = data[0];
    int vmId = data[1];
    int result = data[2];

    if (result == CloudSimTags.TRUE) {
      if (vmId == vms.get(vms.size() - 1).getId()) schedule(getId(), 0, CloudSimTags.NextCycle);
      Log.printLine(CloudSim.clock() + ": " + getName() + ": VM #" + vmId + " has been created in Datacenter #" + datacenterId);
    } else {
      Log.printLine(CloudSim.clock() + ": " + getName() + ": Creation of VM #" + vmId + " failed in Datacenter #" + datacenterId);
    }
  }

  /**
   * Initializes and creates a set of virtual machines with the specified configurations.
   *
   * @param mips      The MIPS rating of each VM.
   * @param pesNumber The number of processing elements (cores) per VM.
   * @param size      The size of the VM (in MB).
   * @param ram       The amount of RAM (in MB).
   * @param bw        The bandwidth (in Mbps).
   * @param vmCount   The number of VMs to create.
   */
  private void initVm(int mips, int pesNumber, long size, int ram, int bw, int vmCount) {
    String vmm = "Xen";
    for (int i = 0; i < vmCount; i++) {
      Vm vm = new Vm(vmId++, getId(), mips, pesNumber, ram, bw, size, vmm, new CloudletSchedulerTimeShared());
      vms.add(vm);
      sendNow(datacenterId, CloudSimTags.VM_CREATE_ACK, vm);
    }
  }

  /**
   * Shuts down the broker and logs the total number of executed cloudlets.
   */
  @Override
  public void shutdownEntity() {
    finishTime = CloudSim.clock();
    Log.printLine(executedCloudlets + " Cloudlets executed.");
    super.shutdownEntity();
  }
}
