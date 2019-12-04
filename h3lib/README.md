#Build Lib & Tests#
You need CMake v3.10 or latter



- **Step 1:** Configure CMAKE and generate makefiles (only once)
    - **`cmake .`**
- **Step 2:** Enter build directory and compile the project
    - **`cd build`**  
    - **`cmake --build .`**
- **Step 3:** Install libraries [Optional]
    - **`sudo make install`**
- **Step 4:** Run tests [Optional] 
    - **`make test`**