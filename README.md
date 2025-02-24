# buzzline-06-stonerogers
Module 6 Project, custom project using producers and consumers 
Melissa Stone Rogers, [GitHub](https://github.com/meldstonerogers/buzzline-06-stonerogers)

## Introduction

This is a professional project creating a custom producer and consumer file. The producer fetches information from [NewsAPI](https://newsapi.org/). The consumer reads this data to an [PostgreSQL](https://www.postgresql.org/download/) database. [pdAdmin 4](https://www.pgadmin.org/download/) was used to view and analyze data.  Ensure both PostgreSQL and pgAdmin4 are installed on your machine before recreating this project. Python Version 3.11.6 was used, as well as Git for version control. 
This project references Dr. Case's project repository found [here](https://github.com/denisecase/buzzline-05-case). Much of the detailed instructions in this README.md were borrowed from Dr. Case's project specifications, and updated for my machine.
Commands were used on a Mac machine running zsh. 

## Task 1. Use Tools from Module 1 and 2

Before starting, ensure you have first completed the setup tasks in [Project 1](https://github.com/denisecase/buzzline-01-case) and [Project 2](https://github.com/denisecase/buzzline-02-case), created by Dr. Case. 
Python 3.11 is required. 

## Task 2. Copy This Example Project and Rename

Once the tools are installed, copy/fork this project into your GitHub account and create your own version of this project to run and experiment with.
Name it `buzzline-04-yourname` where yourname is something unique to you.
Follow the instructions in [FORK-THIS-REPO.md](https://github.com/denisecase/buzzline-01-case/docs/FORK-THIS-REPO.md).

## Task 3. Manage Local Project Virtual Environment

Follow the instructions in [MANAGE-VENV.md](https://github.com/denisecase/buzzline-01-case/docs/MANAGE-VENV.md) to:
1. Create your .venv
2. Activate .venv
3. Install the required dependencies using requirements.txt.

```zsh
python3.11 -m venv .venv
source .venv/bin/activate
```
```zsh
python -m pip install --upgrade pip setuptools wheel
python -m pip install --upgrade -r requirements.txt

```

Remember, each time a new terminal is opened, activate the .venv. 
```zsh
source .venv/bin/activate
```

### Initial Project Commit 
Turn on the autosave function in VS Code. Push changes to GitHub freqently to effectively track changes. Update the commit message to a meaningful note for your changes. 
```zsh
git add .
git commit -m "initial"                         
git push origin main
```

## Task 4. Start Zookeeper and Kafka (2 Terminals)

If Zookeeper and Kafka are not already running, you'll need to restart them.
See instructions at [SETUP-KAFKA.md] to:

1. Start Zookeeper Service ([link](https://github.com/denisecase/buzzline-02-case/blob/main/docs/SETUP-KAFKA.md#step-7-start-zookeeper-service-terminal-1))
### Start Zookeeper Service (Terminal 1)

```zsh
cd ~/kafka
chmod +x zookeeper-server-start.sh
bin/zookeeper-server-start.sh config/zookeeper.properties
```

Keep this terminal open while working with Kafka.

2. Start Kafka ([link](https://github.com/denisecase/buzzline-02-case/blob/main/docs/SETUP-KAFKA.md#step-8-start-kafka-terminal-2))

### Start Kafka (Terminal 2)
```zsh
cd ~/kafka
chmod +x kafka-server-start.sh
bin/kafka-server-start.sh config/server.properties
```
Keep this terminal open while working with Kafka. 

---
## Task 5. Start a New Streaming Application

This will take two more terminals:

1. One to run the producer which writes messages. 
2. Another to run the consumer which reads messages, processes them, and writes them to a data store. 

### Producer (Terminal 3) 

Start the producer to generate the messages. 
For configuration details, see the .env file. 

In VS Code, open a NEW terminal.
Use the commands below to activate .venv, and start the producer. 

```zsh
source .venv/bin/activate
python3 -m producers.kafka_producer_stonerogers
```
```zsh
source .venv/bin/activate
python3 -m producers.producer_stonerogers
```

### Consumer (Terminal 4)

Start an associated consumer. 
In VS Code, open a NEW terminal in your root project folder. 
Use the commands below to activate .venv, and start the consumer. 

```zsh
source .venv/bin/activate
python3 -m consumers.consumer_stonerogers
```
```zsh
source .venv/bin/activate
python3 -m consumers.consumer_nk_stonerogers
```
---
