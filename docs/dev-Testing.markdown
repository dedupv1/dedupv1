Testing

The dedupv1 comes with tons of unit tests.
```text
./dedupv1_test base
./dedupv1_test core
./dedupv1_test dedupv1d
./dedupv1_test contrib
```
# Configure log4cxx

We use log4cxx for logging. For the differen unittest log4cxx is configured by the logging.conf in the unittest dir (e.g. for base in `base/unit_test/logging.conf`). Here you can add a line like the following:

```
log4j.logger.LOGGER_NAME=LOGGER_LEVEL
```

`LOGGER_NAME` is the name of the Logger. It can be found in the source files (but usually not in the Header Files, here we can not Log without some pain). It declared like the following:

```
LOGGER("LOGGER_NAME");
```

For example the `base/src/hash_index.cc` declares `LOGGER("HashIndex");` in such a way.

`LOGGER_LEVEL` can be one of the following:

* ERROR
* WARNING
* INFO
* DEBUG
* TRACE

If we want the HashIndex with LogLevel Trace we would have add the following to `base/unittest/logging.conf`:

```
log4j.logger.HashIndex=TRACE
```

In Systemtests the logging is configured by `/opt/dedupv1/etc/dedupv1/logging.xml`. The file should look like the following:

```
<?xml version="1.0" encoding="UTF-8" ?>
<!DOCTYPE log4j:configuration SYSTEM "log4j.dtd">
<log4j:configuration xmlns:log4j='http://jakarta.apache.org/log4j/'>

        <appender name="FA" class="org.apache.log4j.RollingFileAppender">
                <param name="filename" value="/opt/dedupv1/var/log/dedupv1/dedupv1.log"/>
                <param name="maxBackupIndex" value="12"/>
                <param name="maxFileSize" value="256MB"/>
                <layout class="org.apache.log4j.PatternLayout">
                        <param name="ConversionPattern" value="%d{dd.MM.yyyy HH:mm:ss,SSS} - %5p [%x]  %18c - %-80m - %l%n" />
                </layout>
        </appender>

        <category name="ContainerStorage"><priority value="DEBUG" /></category>
        <root>
                <level value="INFO" />
                <appender-ref ref="FA" />
        </root>


</log4j:configuration>
```

To increase the loglevel of a component you have to add a logger-description as follows:

```
<?xml version="1.0" encoding="UTF-8" ?>
<!DOCTYPE log4j:configuration SYSTEM "log4j.dtd">
<log4j:configuration xmlns:log4j='http://jakarta.apache.org/log4j/'>

        <appender name="FA" class="org.apache.log4j.RollingFileAppender">
                <param name="filename" value="/opt/dedupv1/var/log/dedupv1/dedupv1.log"/>
                <param name="maxBackupIndex" value="12"/>
                <param name="maxFileSize" value="256MB"/>
                <layout class="org.apache.log4j.PatternLayout">
                        <param name="ConversionPattern" value="%d{dd.MM.yyyy HH:mm:ss,SSS} - %5p [%x]  %18c - %-80m - %l%n" />
                </layout>
        </appender>

        <category name="ContainerStorage"><priority value="DEBUG" /></category>
        <root>
                <level value="INFO" />
                <appender-ref ref="FA" />
        </root>


        <logger name="LOGGER_NAME">
                <level value="LOGGER_LEVEL"/> 
        </logger>
</log4j:configuration>
```


# RAM Disk

To speed up tests and for some evaluations it can be a good idea to use a RAM-Disk. This can be done by two ways.

## tempfs

The easier way is to use tempfs:

```text
   sudo mount -t tmpfs none core/unit_test/work -o size=256m
   ./dedupv1_core_test
   sudo umount core/unit_test/work
```

As this needs root rights we can not integrate this into test scripts.

## ramdisk

You can also create a real ramdisk, which bahaves like a normal block device. We also use this way for some kind of performance evaluation, as the ramdisk can be configured to use the whole main memory, so that it can not be used for other purpose.

At first you need to configure the size of the RAM-Disk. Therefore you need to add a boot param to the kernel. First calculate the size the RAM-Disk should have in KB, and keep in mind, that your machine should have more RAM then the RAM-Disk takes... For a 80 GB RAM Disk you get 80 * 1024 * 1024 = 83886080. Now edit `/etc/default/grub`. There you will find the definition of the variable `GRUB_CMDLINE_LINUX_DEFAULT`. In our example you have to add the param `ramdisk_size=83886080`. Here an example of such a file:

```text
# This file is sourced by update-grub, and its variables are propagated
# to its children in /etc/grub.d/

GRUB_DEFAULT=0
#GRUB_HIDDEN_TIMEOUT=0
GRUB_HIDDEN_TIMEOUT_QUIET=true
GRUB_TIMEOUT="3"
GRUB_DISTRIBUTOR=`lsb_release -i -s 2> /dev/null || echo Debian`
GRUB_CMDLINE_LINUX_DEFAULT="quiet splash"
GRUB_CMDLINE_LINUX=""

# Uncomment to disable graphical terminal (grub-pc only)
#GRUB_TERMINAL=console

# The resolution used on graphical terminal
# note that you can use only modes which your graphic card supports via VBE
# you can see them in real GRUB with the command `vbeinfo'
#GRUB_GFXMODE=640x480

# Uncomment if you don't want GRUB to pass "root=UUID=xxx" parameter to Linux
#GRUB_DISABLE_LINUX_UUID=true

# Uncomment to disable generation of recovery mode menu entrys
#GRUB_DISABLE_LINUX_RECOVERY="true

# Uncomment to get a beep at grub start
#GRUB_INIT_TUNE="480 440 1"
```

After changing this call `sudo update-grub` and reboot the machine.

When the machine is up you can use `/dev/ram0` in the same way, as you would use any other machine. If you want to reserve the whole memory for the RAM-Disk you need to write it first. For the RAM-Disk deescribed above you do this by:

```text
dd if=/dev/zero of=/dev/ram0 bs=1K count=83886080
```

Next you can build a Filesystem and mount the disk:

```text
mkdir /mnt/ram1
mkfs.ext3 /dev/ram0
mount -o noatime /dev/ram0 /mnt/ram1
```

To create the RAM-Disk automatically at startup of the machine you can add the last three (or four, with reservation of the memory) lines to `/etc/rc.local`. It is also possible to add `chown` and `chmod` commands to make the RAM-Disk usable by standard users.

# Valgrind

You can run the tests with valgrind to detect memory related problems. To do so you need to call the dedupv1_test with the option `--mode=valgrind`. In the tests on jenkins we use `--mode=valgrind-xml` to create the xml output files.

If you found an error, please fix it. If you found a false positive (valgrind says something is wrong, but it is ok) you have to add a rule to the suppression file in `scripts/varlgrind/dedupv1.supp`. This is no easy task (and should not, so you think twice before adding a new rule there). At first you need to find out during which test the problem happens. Say it `TestClass.test_name` in `core`. Then you have to run the test again with `--mode=valgrind-supp`:

```text
./dedupv1_test core --mode=valgrind-supp --gtest_filter=TestClass.test_name --temp_dir=/mnt/ssd1/work
```

Now valgrind asks you for each problem if it should show a suppression rule for it. Press `y` for the rule you which and copy it to our suppression file.

Rules are described at [valgrind.org](http://valgrind.org/docs/manual/manual-core.html#manual-core.suppress). Very often you want to make a rule more general then it comes from valgrind. Then you can replace frames by thre dots, which means any frame.

# Client for automated testing

At the moment we use only Ubuntu 10.10 Clients for automatred testing over network. This clients should have a directory /mnt/ssd1/acronis-images/ where the data have stay, that we shall copy. Each Client needs a running RPyC-Server. The following Packages need to be installed:

    apt-get install open-iscsi screen xfsprogs xfsdump
    
# Further Tipps

 * You can limit the tests to be executed via the @--gtest_filter@ parameter.
