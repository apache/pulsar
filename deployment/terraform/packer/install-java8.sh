#!/bin/bash

# Add the Java 8 repository.
sudo echo "deb http://ppa.launchpad.net/webupd8team/java/ubuntu trusty main" | sudo tee /etc/apt/sources.list.d/webupd8team-java.list
sudo echo "deb-src http://ppa.launchpad.net/webupd8team/java/ubuntu trusty main" | sudo tee -a /etc/apt/sources.list.d/webupd8team-java.list
sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys EEA14886
sudo apt-get update

# Set the "don't bother us" option to install.
echo debconf shared/accepted-oracle-license-v1-1 select true | \
sudo debconf-set-selections
echo debconf shared/accepted-oracle-license-v1-1 seen true | \
sudo debconf-set-selections

# Install Java 8.
sudo apt-get -y install oracle-java8-installer oracle-java8-set-default
