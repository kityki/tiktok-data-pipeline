# TikTok Play Store Reviews Pipeline

## Overview

This project is an automated, end-to-end data engineering pipeline built with Apache Airflow and Docker. It monitors a directory for a dataset of TikTok Google Play reviews, cleans and transforms the data, and automatically loads the processed records into MongoDB using Airflow's modern Data-Aware Scheduling (Assets).

## Pipeline Workflow

1. **File Sensor & Branching:** An Airflow sensor waits for the CSV file. A branching task checks if it is empty.
2. **Data Transformation (TaskGroup):** Replaces `null` values with `-`, sorts by `created_date`.
3. **Data-Aware Loading:** A second DAG automatically triggers to load the clean records into MongoDB.

## Airflow DAG Graph

![Airflow Graph](<Screenshot 2026-02-23 at 01.30.20.png>)

---

## MongoDB Aggregation Results

### 1. Top 5 frequently occurring comments

```javascript
tiktok_db >
  db.reviews.aggregate([
    { $group: { _id: "$content", count: { $sum: 1 } } },
    { $sort: { count: -1 } },
    { $limit: 5 },
  ])[
    // Output:
    ({ _id: "Good", count: 11535 },
    { _id: "Nice", count: 9338 },
    { _id: NaN, count: 7281 },
    { _id: "Nice app", count: 4751 },
    { _id: "Good app", count: 3556 })
  ];
```

### 2. All entries where the “content” field is less than 5 characters long

```javascript
tiktok_db >
  db.reviews.find({
    $expr: {
      $lt: [{ $strLenCP: { $toString: "$content" } }, 5],
    },
  })[
    // Output sample:
    ({
      _id: ObjectId("699b9d11dfd27e63de456302"),
      reviewId: "gp:AOqpTOFOKOx...",
      userName: "OmoAkinola Survey Consult",
      content: "Cool",
      score: 5,
      at: "2022-01-17 10:46:27",
      replyContent: "-",
      repliedAt: "-",
    },
    {
      _id: ObjectId("699b9d11dfd27e63de456303"),
      reviewId: "gp:AOqpTOHLfvI7...",
      userName: "S R",
      content: "Best",
      score: 5,
      at: "2022-01-17 10:46:40",
      replyContent: "-",
      repliedAt: "-",
    })
  ];
```

### 3. Average rating for each day (the result should be in timestamp type).

````javascript
tiktok_db> db.reviews.aggregate([
  {
    $addFields: {
      parsedDate: { $toDate: "$at" },
      numericScore: { $toDouble: "$score" }
    }
  },
  {
    $group: {
      _id: { $dateTrunc: { date: "$parsedDate", unit: "day" } },
      averageRating: { $avg: "$numericScore" }
    }
  },
  { $sort: { _id: 1 } }
])

// Output sample:
[
  {
    _id: ISODate('2022-01-17T00:00:00.000Z'),
    averageRating: 4.359368143922773
  },
  {
    _id: ISODate('2022-01-18T00:00:00.000Z'),
    averageRating: 4.316648531011969
  },
  {
    _id: ISODate('2022-01-19T00:00:00.000Z'),
    averageRating: 4.333502152443657
  },
  {
    _id: ISODate('2022-01-20T00:00:00.000Z'),
    averageRating: 4.337206365243748
  }
]```
````
