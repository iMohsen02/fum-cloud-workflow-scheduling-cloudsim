# Workflow Scheduling in Cloud Computing with CloudSim

## Project Overview

This project focuses on cloud computing and has been developed using the `CloudSim` simulator. The objective is to analyze and compare three different scheduling algorithms for workflow execution in heterogeneous cloud environments.

## Features

-   Utilizes `CloudSim` for cloud environment simulation
-   Implements three different scheduling algorithms:
    1. **Shortest Job First (SJF)**: Assigns tasks to the fastest available machines and executes them based on the shortest execution time
    2. **Level Based (LB)**: Assigns tasks based on their depth in the workflow
    3. **Subsequent Tasks Based (STB)**: Assigns tasks based on the number of dependent tasks
-   Evaluates four standard workflow types:
    -   `CyberShake`
    -   `Ligo`
    -   `Montage`
    -   `Sipht`
-   Analyzes and compares the performance of each algorithm using charts and experiment results

## Execution Guide

### Prerequisites

-   `Java JDK` version 8 or higher
-   `CloudSim 4.0` or later
-   `IntelliJ IDEA` or `Eclipse` for development and execution

### Steps to Run the Project

1. Clone the repository:
    ```bash
    git clone <repository-url>
    cd cloudsim-workflow-scheduler
    ```
2. Open the project in `IntelliJ IDEA` or `Eclipse`
3. Add `CloudSim` to the classpath
4. Run the `Main.java` file

## Project Structure

```
├── src
│   ├── Main.java                # Entry point of the project
│   ├── scheduler
│   │   ├── SJF.java              # Implementation of SJF algorithm
│   │   ├── LB.java               # Implementation of LB algorithm
│   │   ├── STB.java              # Implementation of STB algorithm
│   ├── workflow
│   │   ├── WorkflowParser.java   # Workflow processing
│   │   ├── Task.java             # Task definition
│   │   ├── Datacenter.java       # Datacenter definition
│   ├── broker
│   │   ├── MyBroker.java         # Cloud resource management
├── resources
│   ├── workflows                 # Workflow files in DAX format
│   ├── logs                       # Simulation logs
│   ├── results                    # Execution results
├── README.md
```

## Output Analysis

-   Execution results are stored in the `results` directory.
-   Comparative charts are displayed in `logs` and the console.
-   Performance evaluation is based on the total execution time of workflows.

## Experimental Results

The following table presents the execution times for different scheduling algorithms across various workflow types:

| Workflow / Algorithm | Shortest Job First (SJF) | Level Based (LB) | Subsequent Tasks Based (STB) |
| -------------------- | ------------------------ | ---------------- | ---------------------------- |
| CyberShake           | 25801                    | 25801            | 25801                        |
| Ligo                 | 27603                    | 26400            | 26403                        |
| Montage              | 28200                    | 28200            | 28200                        |
| Sipht                | 30001                    | 28800            | 28800                        |

## Conclusion

This project successfully implements and evaluates three scheduling algorithms using `CloudSim` on various workflows. Results indicate that algorithm performance varies based on workflow characteristics. For instance, **STB** performed better on workflows with high dependencies, whereas **SJF** was more suitable for independent tasks.

## Future Enhancements

-   Implement additional scheduling algorithms
-   Optimize resource allocation in cloud environments
-   Support more complex workflow structures

## Contributors

-   **Mohsen Gholami Golkhatmi**
-   Ferdowsi University of Mashhad

## License

This project is released under the `MIT` License.

## Contact

For any questions or issues, feel free to contact the contributors:

-   **Mohsen Gholami Golkhatmi | iMohsen02**: iMohsen2002@gmail.com
