#!/bin/bash

# Initialize a counter
counter=0

while true; do
  # Increment the counter at the start of each iteration
  ((counter++))

  # Define an array of test commands
  test_commands=(
    "go test -run 2A"
    "go test -run 2B"
    "go test -run TestPersist12C"
    "go test -run TestPersist22C"
    "go test -run TestPersist32C"
    "go test -run TestFigure82C"
    "go test -run TestUnreliableAgree2C"
    "go test -run TestFigure8Unreliable2C"
    "go test -run TestReliableChurn2C"
    "go test -run TestUnreliableChurn2C"
    "go test -run TestSnapshotBasic2D"
    "go test -run TestSnapshotInstall2D"
    "go test -run TestSnapshotInstallUnreliable2D"
    "go test -run TestSnapshotInstallCrash2D"
    "go test -run TestSnapshotInstallUnCrash2D"
  )

  # Loop through each test command
  for cmd in "${test_commands[@]}"; do
    $cmd > output_all.log
    if [ $? -ne 0 ]; then
      echo "Test failed"
      break 2  # Exit both the for-loop and the while-loop
    fi
  done

  # Print the current number of iteration
  echo "Iteration number: $counter"
done
