# High-Throughput Parallel Data Processing & Filtering System

This project is a high-performance data processing pipeline designed to handle large-scale datasets. It utilizes the Producer-Consumer pattern and multi-threading for efficient, real-time data ingestion and processing.

## 🚀 Key Features


- **Multi-threaded Producer-Consumer Architecture**: Segregates data ingestion (reading from archives) from processing (filtering and writing results) to maximize throughput.
- **Advanced String Matching**: Implements and compares two high-performance filtering techniques:
  - **Regex Filtering**: Precise and flexible pattern matching using Python's `re` module.
  - **Aho-Corasick Algorithm**: Optimized multi-pattern searching for ultra-fast sensitive content identification.
- **Performance Benchmarking**: Integrated timing mechanisms track the efficiency of each processing step across various task chunk sizes.
- **Support for Compressed Archives**: Native ability to process data directly from RAR archives without full manual extraction.


- **Multi-threaded Producer-Consumer Architecture**: Engineered a robust parallel system that segregates data ingestion from processing, effectively eliminating I/O bottlenecks and maximizing throughput.
- **Advanced Algorithmic Performance**: Implemented and benchmarked the **Aho-Corasick** algorithm alongside optimized **Regex** models for real-time, multi-pattern string matching.
- **Scalable Data Ingestion**: Native support for processing large-scale datasets directly from compressed archives (RAR) using automated chunking and performance tracking.
- **Data-Driven Analysis**: Integrated benchmarking tools that provide detailed metrics for processing time, resource utilization, and algorithm efficiency.

## 🛠️ Built With

- **Python**: Core logic and asynchronous processing.
- **Pandas**: Efficient data manipulation and CSV handling.
- **PyAhoCorasick**: High-performance multi-pattern string matching.
- **Openpyxl**: Analysis results exported to Excel format.


