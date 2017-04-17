export GOPATH=/c/Projects/golabs-S2017

echo "+==================================================================+" > final_result.out
echo "|                        TEST ORIGINAL 3A                          |" >> final_result.out
echo "+==================================================================+" >> final_result.out
echo " " >> final_result.out

for i in {1..20}
do
  echo "Test(" $i ") ................................................. :-" >> final_result.out
  go test -run 3A >> final_result.out
done

echo "" >> final_result.out
echo "+==================================================================+" >> final_result.out
echo "|                          TEST RACE 3A                            |" >> final_result.out
echo "+==================================================================+" >> final_result.out
echo " " >> final_result.out

for i in {1..20}
do
  echo "Test(" $i ") ................................................. :-" >> final_result.out	
  go test -race -run 3A  >> final_result.out
done

echo " " >> final_result.out
echo "+==================================================================+" >> final_result.out
echo "|                        TEST ORIGINAL 3B                          |" >> final_result.out
echo "+==================================================================+" >> final_result.out
echo " " >> final_result.out

for i in {1..50}
do
  echo "Test(" $i ") ................................................. :-" >> final_result.out
  go test -run 3B >> final_result.out
done

echo "" >> final_result.out
echo "+==================================================================+" >> final_result.out
echo "|                          TEST RACE 3B                            |" >> final_result.out
echo "+==================================================================+" >> final_result.out
echo " " >> final_result.out

for i in {1..50}
do
  echo "Test(" $i ") ................................................. :-" >> final_result.out
  go test -race -run 3B >> final_result.out
done