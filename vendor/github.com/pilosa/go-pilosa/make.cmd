@echo off

REM default target is test
if "%1" == "" (
    goto :test
)

2>NUL call :%1
if errorlevel 1 (
    echo Unknown target: %1
)

goto :end

:cover
    go test -cover -tags=integration    
    goto :end

:generate
    echo Generating protobuf code is not supported on this platform.
    goto :end

:test
    go test
    goto :end

:test-all
    go test -tags=integration
    goto :end

:end
