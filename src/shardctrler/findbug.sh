count=0
while [ $count -lt 100 ]; do
  go test
  count=$((count+1))
  if [ $? -ne 0 ]; then
    echo "Test failed"
    break
  fi
done