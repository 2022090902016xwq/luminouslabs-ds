@echo off
setlocal enabledelayedexpansion

rem 设定测试次数
set /a count=1

rem 设定测试命令和保存结果的目录
set test_command=go test -run  TestBasicAgree2B
set results_dir=TestBasicAgree2B
mkdir %results_dir%

:loop
if %count% leq 1000 (
    echo Running test %count%...
    rem 运行测试并将输出保存到文件中
    %test_command% > %results_dir%\TestBasicAgree2B_%count%.txt
    set /a count+=1
    goto loop
)

echo All tests completed.
