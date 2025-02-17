package scheduller;

import org.jetbrains.annotations.NotNull;
import workflow.Task;
import workflow.Workflow;
import workflow.WorkflowPasser;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * The Scheduler class is responsible for scheduling {@link Task tasks} in a {@link Workflow}
 * based on different {@link SchedulingAlgorithm scheduling algorithms}.
 *
 * @see Workflow
 * @see WorkflowPasser
 * @see scheduller.Scheduler
 * @see Broker.MyBroker
 * @see org.cloudbus.cloudsim.Cloudlet
 * @see MyDatacenter.MyDatacenter
 * @see Task
 *
 * @since 2/7/2025
 * @author Mohsen Gholami | iMohsen02
 */
public class Scheduler {

  /**
   * Enum representing different scheduling strategies.
   */
  public enum SchedulingAlgorithm {
    /** Shortest Job First Scheduling */
    SHORTEST_JOB_FIRST_SJF,
    /** Level-Based Scheduling */
    LEVEL_BASED_LB,
    /** Subsequent Tasks Based Scheduling */
    SUBSEQUENCE_TASK_BASE_STB
  }

  /** The workflow containing tasks to be scheduled */
  private final Workflow workflow;
  /** List of tasks that are ready for execution */
  private final List<Task> readyTasks;
  /** List of child tasks that will be executed next */
  private List<Task> readyChildren;
  /** The selected scheduling strategy */
  private final SchedulingAlgorithm schedulingType;
  /** Counter for executed tasks */
  private int executedTasks;

  /**
   * Constructor to initialize the Scheduler with a given workflow and scheduling type.
   *
   * @param workflow       The workflow containing tasks.
   * @param schedulingType The scheduling algorithm to be used.
   */
  public Scheduler(@NotNull Workflow workflow, SchedulingAlgorithm schedulingType) {
    this.executedTasks = 0;
    this.workflow = workflow;
    this.schedulingType = schedulingType;
    this.readyChildren = new ArrayList<>();
    this.readyTasks = new ArrayList<>();

    // Initialize ready tasks from the workflow (these are roots)
     readyTasks.addAll(workflow.getRoots()); // also ok
//    readyTasks.addAll(workflow.getTasks().values().stream().filter(Task::isReady).toList()); // also ok
  }

  /**
   * Schedules tasks based on the selected scheduling strategy.
   *
   * @param userId The ID of the user executing the tasks.
   *
   * @see #notifyExecuted(Task)
   * @see Task
   * @see SchedulingAlgorithm
   */
  public void Schedule(int userId) {
    // Add newly ready child tasks to the ready queue
    readyTasks.addAll(readyChildren);
    readyTasks.forEach(t -> t.setUserId(userId));

    // Reset ready children list
    readyChildren = new ArrayList<>();

    switch (this.schedulingType) {
      // schedule cloudlet base on Shortest Job First (SJF) algorithm
      case SHORTEST_JOB_FIRST_SJF -> scheduleSJF();
      // schedule cloudlet base on Level Based (LB) algorithm
      case LEVEL_BASED_LB -> scheduleLB();
      // schedule cloudlet base on Subsequent Tasks Based (STB) algorithm
      case SUBSEQUENCE_TASK_BASE_STB -> scheduleSTB();
      // handle other events
      default -> throw new IllegalStateException("Unexpected value: " + this.schedulingType);
    }
  }

  /**
   * Schedules tasks using the Shortest Job First (SJF) algorithm.
   * Tasks are sorted based on their runtime in ascending order.
   */
  private void scheduleSJF() {
    readyTasks.sort(Comparator.comparingDouble(Task::getRuntime));
  }

  /**
   * Schedules tasks using the Level-Based (LB) algorithm.
   * Tasks are sorted based on their level in the workflow.
   */
  private void scheduleLB() {
    readyTasks.sort(Comparator.comparingDouble(Task::getLevel));
  }

  /**
   * Schedules tasks using the Subsequent Tasks Based (STB) algorithm.
   * Tasks are sorted based on the number of their dependent (subsequent) tasks.
   */
  private void scheduleSTB() {
    readyTasks.sort(Comparator.comparingInt(Task::getSubsequentTasksLen).reversed());
  }

  /**
   * Retrieves the list of ready tasks.
   *
   * @return List of tasks that are ready for execution.
   */
  public List<Task> getReadyTasks() {
    return readyTasks;
  }

  /**
   * Notifies the scheduler that a task has been executed.
   * This updates the executed task count and adds its child tasks to the ready list if it is ready.
   *
   * @param executedTask The task that has just been executed.
   */
  public void notifyExecuted(Task executedTask) {
    executedTasks++;
    this.readyChildren.addAll(executedTask.notifyExecute());
  }

  /**
   * Checks if there are remaining tasks to be executed.
   *
   * @return true if there are unexecuted tasks, otherwise false.
   */
  public boolean isTaskRemains() {
    return workflow.getTasks().size() - 1 > executedTasks;
  }
}
