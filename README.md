# Log File Analyzer

##  Project Description
**Log File Analyzer** is a Python-based tool that analyzes application log files to identify errors, warnings, and usage patterns.
It processes log entries, summarizes activity by log level, and generates structured reports for easy review.

This project is created as a **learning and practice project** to understand log parsing, data aggregation, and automation using Python.

---

## Purpose of the Project
This project is:
- A learning exercise  
- A log analysis utility  
- Not a production-grade monitoring system  

It helps in understanding how logs can be analyzed to detect issues and trends.

---

##  Features
- Supports analysis of log files (text-based or structured)
- Identifies and counts log levels (INFO, WARNING, ERROR)
- Detects frequent or repeated error messages
- Generates summary reports in CSV format
- Handles malformed or incomplete log entries safely

---

##  Tech Stack
- **Language:** Python  
- **Libraries:** pandas, re, datetime  
- **Execution:** Command Line (CLI)  
- **Input Format:** Log files  
- **Output Format:** CSV  

---

## Architecture Overview
- Log files are read line by line
- Regular expressions are used to parse log entries
- Log messages are categorized by severity level
- Aggregated statistics are generated
- Results are written to CSV reports

---

##  Project Structure

logfile-analyzer/
│
├── log_analyzer.py # Main log analysis script

├── requirements.txt # Python dependencies

├── README.md # Project documentation

│

├── logs/

│ └── sample.log # Sample input log file
│
└── reports/

└── summary.csv # Generated analysis report

---

##  Installation Steps

1. Clone the repository:

   git clone <repository-url>
   cd logfile-analyzer

2. (Optional) Create a virtual environment:
        python -m venv venv
3. Activate the virtual environment:
        venv\Scripts\activate
4. Install dependencies:
         pip install -r requirements.txt
5. Run the Analyzer
         python log_analyzer.py --input logs/sample.log --outdir reports
6. Input Log File
         2024-01-01 10:00:01 INFO Application started
         2024-01-01 10:05:12 ERROR Database connection failed
         2024-01-01 10:06:45 WARNING Low memory
   
## Screenshots Terminal output
<img width="1326" height="755" alt="Screenshot 2026-01-05 100404" src="https://github.com/user-attachments/assets/64035292-e322-49f7-b655-50b2206e605c" />


## Output 

The analyzer generates a CSV report summarizing log activity.

reports/summary.csv

level,count
INFO,1
ERROR,1
WARNING,1

## Data Handling

Safely handles missing or malformed log lines

Ignores lines that do not match expected format

Produces clean and structured output

## Testing

Automated tests are not currently included.

Recommended future improvement:

Add unit tests using pytest

Test different log formats and edge cases

 ## Code Quality

Modular and readable code

Easy to extend with new parsing rules

Ready for logging and linting improvements

## Conclusion

Log File Analyzer is a simple Python project that demonstrates:

Log parsing and analysis

Data aggregation

CLI-based automation

It is suitable for learning, practice, and portfolio demonstration.
