# course-tuwien-data-intensive-computing

Project work done as part of the course **Data Intensive Computing** at the Vienna University of Technology (TU Wien). This repository contains distributed computing implementations focused on large-scale data processing using Hadoop and Python.

## Project Structure

The project is organized into independent assignment modules using the following structure:

```text
.
├── assignment1
│   ├── data/               # Local samples and stopwords
│   ├── doc/                # Course instructions and task details
│   ├── src/                # Implementation logic
│   │   ├── script.py       # Python source code
│   ├── run_local.sh        # Local test execution script
│   └── run_prod.sh         # Cluster execution script
├── requirements.txt        # Python dependencies
└── README.md
```

## Setup

1. Install Python 3
2. Install dependencies using `pip install -r requirements.txt`

## Execution

Run the pipeline by executing one of the provided shell scripts.

```bash
cd assignment1
./run_local.sh
```

The results are placed in the `result` directory.

