## COSTS FOR SCALABILITY
To calculate the different costs, Azure Pricing Calculator is the best option since the project is orchestrated in Azure.
Minimum for calculating the price is 5gb

Gathering the data of each municipality is not a easy quest, so the backups are KEY

![X1 prices](<diagram_images/Captura de pantalla 2026-01-09 171809.png>)
The current database contains approximately 130,000 rows for Spain, occupying 41.56 MB of storage in Azure SQL Database. This real measurement, obtained directly from the Azure Portal, is used as the baseline (x1) to infer storage requirements and costs for larger data volumes.

### x1 Data Volume (≈ 130,000 rows – Spain)
Measured storage usage: ~42 MB
Assigned storage: 80 MB
Maximum storage: 1 GB
Compute tier: Azure SQL Database – General Purpose
vCores: 2 vCores
Usage pattern: Daily usage
Backups: Automatic backups enabled (mandatory)
Spain Central
ALMOST FREE

### x10 Data Volume (≈ 1.3 million rows)
Estimated storage: ~420 MB
Compute tier: Azure SQL Database – General Purpose
vCores: 2 vCores
Usage pattern: Daily usage
Backups: Automatic backups enabled (mandatory)
Spain Central
ALMOST FREE

### x100 Data Volume (≈ 13 million rows)
Estimated storage: ~4.2 GB
Compute tier: Azure SQL Database – General Purpose
vCores: 2–4 vCores
Usage pattern: Daily usage with higher ETL workload
Backups: Automatic backups enabled (mandatory)
Spain Central
Estimated monthly cost: [(calculated using Azure Pricing Calculator)](https://azure.com/e/ede3d428d44c4817ab01f323c95b68a2)

### x1,000 Data Volume (≈ 130 million rows)
Estimated storage: ~42 GB
Compute tier: Azure SQL Database – General Purpose / Business Critical
vCores: 4–8 vCores
Usage pattern: Daily usage with heavy ETL and analytical querie
Backups: Automatic backups enabled (mandatory)
Spain Central
Estimated monthly cost: [(calculated using Azure Pricing Calculator)](https://azure.com/e/8e3b66cc3ef84717aa24e6c62719e96d)

### x10⁶ Data Volume (≈ 130 billion rows)
Estimated storage: ~42 TB
Compute tier: Not suitable for a single Azure SQL Database
vCores: 16+ vCores or distributed compute
Usage pattern: Continuous usage at global scale
Backups: Automatic backups enabled (mandatory)
Estimated monthly cost: NOT POSSIBLE TO CALCULATE
So the data should be migrated to other service.

At this scale, Azure SQL Database is no longer cost-effective. A distributed architecture based on Azure Data Lake Storage + Azure Synapse Analytics or Azure Databricks would be required.

### Final Considerations
Using real Azure metrics shows that storage grows linearly with data volume and remains relatively inexpensive even at x1,000 scale. However, compute resources (vCores) are the primary cost driver as data volume and workload increase. Therefore, scalability must be addressed through incremental loading, partitioning strategies, and optimized data processing rather than storage expansion alone.