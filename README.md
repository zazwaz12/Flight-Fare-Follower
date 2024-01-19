# Flight Fare Data Tracker

## Objective

This project is dedicated to developing a robust and comprehensive dataset, specifically engineered for use by data analysts and scientists in the travel industry. Our focus is on the intricacies of data engineering, demonstrating our expertise in gathering, processing, and structuring data to facilitate advanced airfare strategy development. This dataset will serve as a crucial asset for travel agencies, aiding in the optimization of ticket pricing and providing exceptional value to travelers.

## Consumers

Who will benefit most from this dataset, and what are their data interaction preferences?

- Data Analysts and Scientists in the Aviation and Travel Sector: These professionals require a reliable and rich source of data to extract meaningful insights regarding flight pricing trends and patterns.
- Tech-savvy Individuals Monitoring Airfare Trends: A segment of users who rely on data accuracy and comprehensiveness to track and predict airfare changes effectively.

## Questions

What questions are you trying to answer with your data? How will your data support your users?

- What are the cheapest countries to visit during December?
- What are the best days to fly on?
- What is the correlation between exchange rate and flight prices?
- What are the effects of oil prices on flight prices?
- What is the most expensive / cheapest price to Hong Kong if I were to spend 9 days?

## Source datasets

What datasets are you sourcing from? How frequently are the source datasets updating?

- TBD

Example:

| Source name        | Source type         | Source documentation |
| ------------------ | ------------------- | -------------------- |
| Customers database | PostgreSQL database | -                    |
| Orders API         | REST API            | -                    |

## Solution architecture

How are we going to get data flowing from source to serving? What components and services will we combine to implement the solution? How do we automate the entire running of the solution?

- What data extraction patterns are you going to be using?
- What data loading patterns are you going to be using?
- What data transformation patterns are you going to be performing?

We recommend using a diagramming tool like [draw.io](https://draw.io/) to create your architecture diagram.

Here is a sample solution architecture diagram:

![images/sample-solution-architecture-diagram.png](images/sample-solution-architecture-diagram.png)

## Breakdown of tasks

How is your project broken down? Who is doing what?

- Justin:
  - setup for api call of flight data (1/15, done)
  - extract and load part of flight data (1/15, done)
  - docker setup (1/18, done)
  - transform (TODO: 1/22)
- Tim:
  - setup for api call of exchange rate data (1/15, done)
  - logging (1/18, done)
  - unit tests (1/18, done)
  - transform (TODO: 1/22)
- Ahmed:
  - setup for api call of oil prices (1/15, done)
  - extract and load of oil price and/or exchange rate data (1/18, done)
  - transform (TODO: 1/18)
  - AWS (TODO: later)
