# Project plan

## Objective

The goal of our project is to create a comprehensive dataset tailored for data analysts and data scientists working in travel agencies. This dataset will be instrumental in refining plane pricing strategies, particularly in identifying the most cost-effective flight date combinations for travelers.

## Consumers

What users would find your data useful? How do they want to access the data?

Example:

> The users of our datasets are Data Analysts and the Production team in the business.

## Questions

What questions are you trying to answer with your data? How will your data support your users?

Example:

> - How many orders are there for each customer?
> - What countries and regions have the most orders?
> - What customers have their orders delayed?
> - How many delayed orders are there for each country and region?
> - How many orders do we have for each day?
> - How many delayed orders do we have for each day?

## Source datasets

What datasets are you sourcing from? How frequently are the source datasets updating?

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
  - extract and load part of flight data (1/15)
  - docker (TODO: 1/18)
  - transform (TODO: 1/18)
- Tim:
  - setup for api call of exchange rate data (1/15, done)
  - logging (TODO: 1/18)
  - unit tests (TODO: 1/18)
  - transform (TODO: 1/18)
- Ahmed:
  - setup for api call of oil prices (1/15, done)
  - extract and load of oil price and/or exchange rate data (TODO: 1/18)
  - transform (TODO: 1/18)
  - AWS (TODO: later)

We recommend using a free Task board such as [Trello](https://trello.com/). This makes it easy to assign and track tasks to each individual.

Example:

![images/kanban-task-board.png](images/kanban-task-board.png)
