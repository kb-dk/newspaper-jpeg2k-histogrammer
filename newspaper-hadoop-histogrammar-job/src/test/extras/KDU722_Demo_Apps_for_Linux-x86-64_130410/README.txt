These demo applications have been built using GCC version 4.7.2 for a
64-bit Linux system -- specifically tested for Ubuntu.

The only demo application not available for Linux is the powerful
"kdu_show" app, which is available only on Windows and Mac platforms
at the present time.

To use these demo executables, you will need to edit your ".profile"
file to add the current directory to your executable PATH variable
and also to the LD_LIBRARY_PATH variable.  Below is an example of
how this might be done for the typical BASH shell.

PATH=$PATH:~/kakadu/bin
LD_LIBRARY_PATH=$LD_LIBRARY_PATH:~/kakadu/bin
export PATH
export LD_LIBRARY_PATH


