export GOPATH=/c/Projects/golabs-S2017

echo "+==================================================================+" > final_result.out
echo "|                        TEST ORIGINAL 3C                          |" >> final_result.out
echo "+==================================================================+" >> final_result.out
echo " " >> final_result.out

for i in {1..100}
do
  echo "Test(" $i ") ................................................. :-" >> final_result.out	
  go test -run 3C  >> final_result.out
done