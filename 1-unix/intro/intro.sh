#!/usr/bin/env bash

# This is a bash script file.
# To run this file use ./intro.sh in your terminal while in the correct directory.

# Echo prints lines to the output, if run using ./intro.sh then the output will be the terminal
echo "Running intro.sh"

# The command cd (change directory) can be used to change the current directory.
# Update the cd command below to make sure that the rest of this script is executed in the apacheLog folder that is inside of the data folder.
cd ../data/apacheLog || exit
# The '||' on this line is like a or. If the left part gives an exit code of non-zero (error/false) the right part is evaluated.
# We also have the '&&' sign. this only evaluates the right part if the left part has a zero exit code (no error/true).
echo "Moved from intro to the data->apacheLog folder"
# The following line will execute the 'ls' command and save it in the variable lsOutput
lsOutput=$(ls)
# Then we can use echo to print the result
echo "Result of ls"
echo "$lsOutput"

# Implement a command that displays all files and their details in the current directory
# Do this by typing it between the brackets below.
# You can first test individual commands by pasting them in the terminal.
# (Make sure that you are in the correct directory if you want to test it)
detailedLsOutput=$(ls -l)
# Prints the lsOutput
echo "Files in directory:"
echo "$detailedLsOutput"

# Implement a command that looks for all files containing '_log' in their name
logFiles=$(find . -name "*_log*")
# Print result
echo "Log files:"
echo "$logFiles"

# Implement a command that gets all the lines that contain "File does not exist" from the 'error_log' file
lines=$(grep -F "File does not exist" error_log)
# Print result
echo "Lines from error_log where 'File does not exist:'"
echo "$lines"

# Now that you have used the basics of finding files and getting text from files it's time to move on.
# The real power of these commands are in the ability to combine them.
# Using the output of the first command  as data for the second  is called piping and is denoted by the symbol |
# A silly example is "ls | lolcat"
echo "The ls command but now with some more color"
ls | lolcat

# Lets create an pipeline where we get the first 3 GET requests that where requested from the access_log.
# the output of a line should look like: GET /twiki/bin/edit/Main/Double_bounce_sender?topicparent=Main.ConfigurationVariables HTTP/1.1
# The first part should take all the lines containing "GET" from the 'access_log' file.
# The second part should only keeps the first 3 lines.
# lastly we need to get the data that is between quotes
firstPipeline=$(grep "GET" access_log | head -n3 | grep -o "\".*\"")
echo "First pipeline results:"
echo "$firstPipeline"

# Implement a pipeline that displays only the owner of the log files in the current directory
secondPipeline=$(ls -l | grep "log")
# Print result
echo "Second pipeline results:"
echo "$secondPipeline"

# end on start path
# This is needed for the automated testing
cd ../../intro || exit
