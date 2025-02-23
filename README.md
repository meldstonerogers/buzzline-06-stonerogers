# buzzline-06-stonerogers
Module 6 Project, custom project using producers and consumers 
Melissa Stone Rogers, [GitHub](https://github.com/meldstonerogers/buzzline-06-stonerogers)

## Introduction

This is a professional project incorporating a relational data store. This project uses Apache Kafka to create uniquie JSON producers and consumers to simulate streaming data. Python Version 3.11.6 was used, as well as Git for version control. 
This project was forked from Dr. Case's project repository found [here](https://github.com/denisecase/buzzline-05-case). Much of the detailed instructions in this README.md were borrowed from Dr. Case's project specifications, and updated for my machine.
The sample project uses SQLite and was modified to use MongoDB.
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
