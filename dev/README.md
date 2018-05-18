## Apache Pulsar Dev Tools

### Running Integration Tests on macOS

Currently all the integration tests are docker based and written using arquillian framework. 
Due to the networking issues, the integration tests can only be run on linux environment.
For people who is using macOS as their development environment, you can use [Vagrant](https://www.vagrantup.com)
to launch a linux virtual machine and run the integration tests there.

1. [Download and Install](https://www.vagrantup.com/downloads.html) Vagrant.

2. Provision and launch the dev vm.

   ```shell
   $ cd ${PULSAR_HOME}/dev
   
   # provision the vm
   $ vagrant up
   ```

3. The dev vm will try to mount your current pulsar workspace to be under `/pulsar` in the vm. You might
   potentially hit following errors due to fail to install VirtualBox Guest additions.

   ```
   /sbin/mount.vboxsf: mounting failed with the error: No such device
   ```

   If that happens, follow the below instructions:

   ```
   # ssh to the dev vm
   $ vagrant ssh

   [vagrant@bogon pulsar]$ sudo yum update -y
   [vagrant@bogon pulsar]$ exit  

   # reload the vm
   $ vagrant reload
   ```

4. Now, you will have a pulsar dev vm ready for running integration tests.

   ```shell
   $ vagrant ssh

   # once you are in the pulsar dev vm, you can launch docker.
   [vagrant@bogon pulsar]$ sudo systemctl start docker

   # your pulsar workspace will be mount under /pulsar
   [vagrant@bogon pulsar]$ cd /pulsar

   # you can build and test using maven commands
   ```
