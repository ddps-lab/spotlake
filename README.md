# SpotLake [![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.7084392.svg)](https://doi.org/10.5281/zenodo.7084392)

## What is SpotLake System?
SpotLake system is an integrated data archive service that provides spot instance datasets collected from diverse public cloud vendors.The datasets include various information about spot instances like spot availability, spot interruption frequency, and spot price. Researchers and developers can utilize the SpotLake system to make their own system more cost-efficiently. SpotLake system currently provides the latest and restricted range of spot datasets collected from AWS, Google Cloud, and Azure through a demo page. We believe numerous systems could achieve a huge improvement in cost efficiency by utilizing the SpotLake system.

## About Code
```
.
├── analysis : analysis spot instance's data
├── collector : codes to collect spot instance's data (in aws, azure and gcp)
│   ├── instance-specs : codes for measuring spot instance's hardware specifications
│   │   ├── aws
│   │   ├── azure
│   │   └── gcp
│   └── spot-dataset : codes to collect spot instance's stability data
│       ├── aws
│       ├── azure
│       └── gcp
├── frontend : codes of https://spotlake.ddps.cloud
│   ├── build
│   ├── public
│   └── src
├── preprocessing : preprocess data to analysis
└── utility : etc tools
```
## Paper and Demo-Web
If you are interested in an analysis of the SpotLake datasets or system implementation, check the latest version of the SpotLake paper which is published in IISWC 2022. We also published an older version of the paper through arXiv.

[spotlake-ieee-official.pdf](https://github.com/ddps-lab/spotlake/files/9962402/879800a242.pdf)<br>
https://ieeexplore.ieee.org/document/9975369

Demo-Web is provided so that users can easily access SpotLake data.

https://spotlake.ddps.cloud/

### How To Use Demo-Web

![image](https://user-images.githubusercontent.com/66048830/200404154-54291253-f958-418c-98a7-c3126611d48f.png)

1. **Vendor selection**
<br>On the demo page, users can select one cloud vendor among AWS, Google Cloud, or Azure to show the latest spot instance dataset in the table below. The table shows the latest dataset of the selected cloud vendor, and it contains every pair of instance types and regions provided by the vendor.
2. **Querying**
<br>Since the default table shows only the latest dataset of every instance-region pair, users have to query with specific Instance Type, Region, AZ, and Date Range options to get the historical dataset. Data query has some limitations; the maximum number of the returned data point is 20,000 and user can set the date range up to 1 month. If user selects the ‘ALL’ option in Region or AZ field, the returned dataset contains every Regions or AZs corresponding to the Instance Type option.<br>Even if user send query with specific date range, SpotLake does not return data points in the date range. SpotLake system only saves the data point when there is a change in any fields. Therefore, user only get the changed data points with demo page’s querying feature. If you want to get the full dataset, check the ‘How to access full dataset’ section on about page.
3. **Filtering**
<br>User can apply additional filter to the table that shows default latest dataset or queried dataset. For instance, user can select specific data points that contains specific character in Instance Type column or filter by size of the score. Also table could be exported in the CSV format with EXPORT button.

## How to Access Full Dataset
We can not provide the full dataset through this web-service because the dataset is too large. Those who want to access the full dataset of the SpotLake system, please fill out the google form below and we will give you access permission for the full dataset.

https://forms.gle/zUAqmJ4B9fuaUhE89
