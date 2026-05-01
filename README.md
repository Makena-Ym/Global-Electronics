# 📊 Global Electronics Strategic Analytics: End-to-End BI Project
### Developed by Yvonne M-Gatere

[![Portfolio](https://img.shields.io/badge/Portfolio-Visit%20My%20Site-blue?style=for-the-badge&logo=googlechrome&logoColor=white)](https://makena-ym.github.io/My-Portfolio/)
[![Email](https://img.shields.io/badge/Email-Contact%20Me-red?style=for-the-badge&logo=gmail&logoColor=white)](mailto:makena.gatere@gmail.com)

## 🎯 Business Objective
This project transforms raw, fragmented retail data into a high-performance **Executive Decision Support System**. The goal was to provide the CEO of a global electronics retailer with deep insights into **revenue growth**, **logistics efficiency**, and **market penetration**.

## 🏗️ Technical Architecture: The Medallion Approach
I implemented a **Medallion Architecture** using SQL Server to ensure data integrity and performance:

* **Bronze Layer (Raw):** Ingested 5+ CSV files using `BULK INSERT` and SSMS Import Wizards.
* **Silver Layer (Cleansed):** Data type casting, handling `NULL` values, and "Feature Engineering" (creating `DeliveryDays` and `StoreAgeYears`).
* **Gold Layer (Curated):** Developed complex SQL Views to serve as a **Single Version of the Truth** for Power BI, reducing DAX complexity and increasing report speed.

## 🚀 Key Insights Generated
* **Logistics Breakthrough:** Successfully tracked a reduction in average delivery time from **7.29 days to 3.83 days** (a 47% improvement).
* **Product Performance:** Identified that while "Computers" lead in volume, "Audio & Music" maintain the highest profit efficiency.
* **Market Strategy:** Analyzed geographic data to find that the US market holds **53.4% of orders**, while European hubs offer higher delivery efficiency.

## 🛠️ Tools & Technologies
- **SQL Server (T-SQL):** Data Engineering, Views, and Schema Design.
- **Power BI:** Data Modeling, DAX (Measures), and Star Schema.
- **Canva:** Custom UI/UX Design for the dashboard background.
- **UX/UI Design:** Sidebar navigation, conditional formatting, and sparkline integration.

## 📊 Dashboard Preview

### 1. Executive Summary
Designed for C-suite executives, featuring high-level KPIs, sparklines for profit trends, and category-level profitability.

### 2. Logistics & Operations
Focuses on supply chain reliability, brand performance, and shipping status (On-Time vs. Late).

### 3. Geographic Deep-Dive
A visual representation of global revenue distribution and store performance metrics.

---

## 📖 Data Dictionary & Methodology
To ensure transparency, I included a built-in **Data Dictionary** in the dashboard, defining every metric (Net Profit, Gross Revenue, etc.) and the business logic behind them.

## 🤝 Connect with Me
I am a Data Analyst passionate about turning complex data into actionable stories.

* **Portfolio:** [makena-ym.github.io/My-Portfolio/](https://makena-ym.github.io/My-Portfolio/)
* **Email:** [makena.gatere@gmail.com](mailto:makena.gatere@gmail.com)

---
*Created with ❤️ by Yvonne M-Gatere*
