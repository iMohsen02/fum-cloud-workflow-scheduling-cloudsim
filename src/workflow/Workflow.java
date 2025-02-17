package workflow;

import org.cloudbus.cloudsim.Cloudlet;
import scheduller.Scheduler;

import java.util.*;

/**
 * Represents a workflow consisting of tasks and dependencies between them.
 * It provides methods for adding tasks, defining {@link Dependency dependencies}, and preprocess the task graph like {@link #processLevel() level based processing} or {@link #processSubsequenceTask() discover subsequnece task}.
 * easy to use in your custom workflow parser by {@link  Dependency} and other useful functions
 *
 * @author Mohsen Gholami | iMohsen02
 * @see WorkflowPasser
 * @see scheduller.Scheduler
 * @see Broker.MyBroker
 * @see org.cloudbus.cloudsim.Cloudlet
 * @see MyDatacenter.MyDatacenter
 * @see Task
 * @since 2025
 */
public class Workflow {
  /**
   * Map of tasks indexed by their DAX ID
   */
  private final Map<String, Task> tasks;
  /**
   * List of tasks with no parents (starting tasks)
   */
  private final List<Task> roots;
  /**
   * List of tasks with no children (ending tasks)
   */
  private final List<Task> tails;

  /**
   * Represents a dependency between tasks, where a child task depends on one or more parent tasks. fallow dax rules.
   * it is useful like a bridge between {@link Workflow} and any parser to communicate.
   *
   * @see WorkflowPasser
   * @see Workflow
   */
  public static class Dependency {
    /**
     * ID of the dependent (child) task
     */
    private String child;
    /**
     * List of parent task IDs
     */
    private final List<String> parents;

    {
      this.parents = new ArrayList<>();
    }

    /**
     * Adds a parent task to the dependency.
     *
     * @param parent The parent task ID.
     */
    public void addParents(String parent) {
      this.parents.add(parent);
    }

    /**
     * Sets the child task ID for this dependency.
     *
     * @param child The child task ID.
     */
    public void setChild(String child) {
      this.child = child;
    }

    @Override
    public String toString() {
      return "Dependency{" +
        "child='" + child + '\'' +
        ", parents=" + parents +
        '}';
    }
  }

  /**
   * Constructs an empty workflow.
   */
  public Workflow() {
    this.tasks = new HashMap<>();
    this.roots = new ArrayList<>();
    this.tails = new ArrayList<>();
  }

  /**
   * Adds a task to the workflow.
   *
   * @param t The task to add.
   */
  public void addTask(Task t) {
    tasks.putIfAbsent(t.getDaxId(), t);
  }

  /**
   * Adds a dependency between tasks.
   *
   * @param dependency The dependency object specifying the parents-child relationship(like DAX workflow).
   */
  public void addDependency(Dependency dependency) {
    Task childTask = tasks.get(dependency.child);
    for (String parent : dependency.parents) {
      Task parentTask = tasks.get(parent);
      childTask.addParent(parentTask);
      parentTask.addChild(childTask);
    }
  }

  /**
   * Preprocess the workflow to determine roots, tails, levels, and subsequent tasks base on its algorithm
   *
   * @param algorithm determine its preprocess base on algorithm if necessary
   */
  public void preprocess(Scheduler.SchedulingAlgorithm algorithm) {
    for (Task task : tasks.values()) {
      if (task.getParents().isEmpty()) roots.add(task);  // discover workflow roots
      if (task.getChildren().isEmpty()) tails.add(task); // discover workflow tails
    }

    // preprocess necessary attributes base on algorithm
    switch (algorithm) {
      // preprocess level of each task in workflow
      case LEVEL_BASED_LB -> processLevel();
      // preprocess subsequence task in workflow
      case SUBSEQUENCE_TASK_BASE_STB -> processSubsequenceTask();
    }

    // print task and its attributes
    List<Task> values = new ArrayList<>(getTasks().values().stream().toList());
    values.sort(Comparator.comparingInt(Cloudlet::getCloudletId));
    values.forEach(t -> System.out.printf("id: \"%s\"\truntime: %-10s length: %-10s level: %-5s stb: %-5s\n", t.getDaxId(), t.getRuntime(), t.getCloudletLength(), t.getLevel(), t.getSubsequentTasksLen()));
  }

  /**
   * Determines the level of each task in the workflow hierarchy.
   */
  private void processLevel() {
    Queue<Task> queue = new ArrayDeque<>(roots);
    for (Task h : roots) {
      h.setLevel(1); // roots are in first level
      queue.addAll(h.getChildren()); // add next level to queue
    }

    while (!queue.isEmpty()) {
      Task task = queue.poll();
      List<Task> levelnessParents = task.getParents().stream().filter(t -> t.getLevel() < 0).toList();

      if (levelnessParents.isEmpty()) { // if all parent has level
        task.setLevel(task.getParents().stream().mapToInt(Task::getLevel).max().orElse(0) + 1); // calculate task level (max parent level + 1)
        queue.addAll(task.getChildren()); // go on and process its children level
      } else queue.add(task); // if there is a parent which has not processed its level, go back to the queue and wait for parent
    }
  }

  /**
   * Processes and assigns subsequent tasks for each task in the workflow.
   */
  private void processSubsequenceTask() {
    Queue<Task> queue = new ArrayDeque<>(tails);
    for (Task tail : tails) queue.addAll(tail.getParents()); // add prev level to queue

    while (!queue.isEmpty()) {
      Task task = queue.poll();
      boolean isChildDone = task.getChildren().stream().allMatch(Task::isDoneSubSequence);

      if (isChildDone) { // all children of task processed their subsequence tasks or not
        task.getChildren().forEach(child -> task.addSubsequenceTasks(child.getSubsequentTasks())); // add all children subsequence to the task subsequence
        task.addSubsequenceTasks(new HashSet<>(task.getChildren())); // add task children to the subsequence
        queue.addAll(task.getParents()); // add prev level to process
      } else queue.add(task); // if there is any children which has not processed its subsequence, go back to the queue and wait for children
    }
  }

  /**
   * Retrieves a task by its DAX ID.
   *
   * @param daxId The DAX ID of the task.
   * @return The corresponding task, or null if not found.
   */
  public Task getTask(String daxId) {
    return tasks.get(daxId);
  }

  /**
   * Gets the number of tasks in the workflow.
   *
   * @return The total number of tasks.
   */
  public int getTaskLength() {
    return tasks.size();
  }

  /**
   * Gets the list of root tasks (tasks with no parents).
   *
   * @return A list of root tasks.
   */
  public List<Task> getRoots() {
    return roots;
  }

  /**
   * Gets the list of tail tasks (tasks with no children).
   *
   * @return A list of tail tasks.
   */
  public List<Task> getTails() {
    return tails;
  }

  /**
   * Gets the map of tasks in the workflow.
   *
   * @return A map of tasks indexed by their DAX ID.
   */
  public Map<String, Task> getTasks() {
    return tasks;
  }

  @Override
  public String toString() {
    return "Workflow{" +
      " tasks=" + getTaskLength() +
      ", roots=" + roots.size() +
      ", tails=" + tails.size() +
      '}';
  }
}