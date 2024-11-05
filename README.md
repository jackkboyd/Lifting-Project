# Weight Lifting Data Pipeline

## Overview
This repository contains a personal project designed to enhance my skills / gain exposure in data engineering. It focuses on creating data pipelines that pull data from an excel doc that tracks all of my weight lifting data. By moving this data into a relational database and then performing some analytics on top of this I'm hoping to gain more insight into my weightlifting journey.

## Tools
I'm using a postgres database hosted on an AWS RDS server. In the future I will be pushing this data to snowflake for more advanced analytics. Finally I am planning on creating a dashboard with PowerBI. I am also planning on scheduling updates with apache airflow and will do some data transformation before importing to snowflake with DBT. 

## Pipelines 
import_factLifts: Data is extracted from my excel document that contains all reps and weights associated with my workouts. This import is run first and not only imports fact data from my workouts, but also creates new dimension members. These dimension members are created via the createNewMembers function and are placed on their respective dimension tables with blank attributes. 

Along with this I've also implemented replace and append logic to determine how historical data interacts with the next upload. In this case replace keys are defined to determine data that should be 'replaced' by the new import, while data that does not match on the replace key gets appended to the table

import_factWeights: Data is extracted from my excel docuemnt that contains all of my weights. This imports is run second and only imports fact data. 

Similar to import_factLifts, this script leverage the replace and append logic

import_dimRoutines: This data is loaded after the fact data and merges on the dimension members that have already been created, populating all of their attributes. I've decided to merge here, instead of merge then append data to keep the size of dimension tables as small as possible.

Along with this, new members are created for workouts. 

import_dimWorkouts: This is the final import and populates the attributes on lift.DimWorkouts. This merges onto the already created data similar to import_dimRoutines. 