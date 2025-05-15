# smart-store-brittany

To get this project running, on Windows follow the following instructions.

### 1. Activate your environment

You must first activate `.venv` by running this command:

```sh
.\.venv\Scripts\Activate
```

**Note:** you may experience this error:

```sh
PS C:\Repos\smart-store-brittany> .\.venv\Scripts\activate
.\.venv\Scripts\activate : File C:\Repos\smart-store-brittany\.venv\Scripts\Activate.ps1 cannot be loaded because running scripts
is disabled on this system. For more information, see about_Execution_Policies at https:/go.microsoft.com/fwlink/?LinkID=135170.
At line:1 char:1

 .\.venv\Scripts\activate

 + CategoryInfo          : SecurityError: (:) [], PSSecurityException
 + FullyQualifiedErrorId : UnauthorizedAccess
```

If so, do the following:

1.  Open Windows Start Menu
2.  Search for Power Shell, right-click and select "Run As Administrator"
3.  Run this command:

    ```sh
    Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
    ```

4.  Confirm with `Y` when prompted
5.  Try running the initial "Activate" command above now. You should no longer see the error.

### 2. Install dependencies

Once our environment is activated we now need to install any dependencies we have listed in `requirements.txt`. To do so, run the following command:

```sh
pip install -r requirements.txt
```

If an update is necessary, run:

```sh
pip install --upgrade pip
```

### 3. Prep the data

To prepare/check our data, we have a script called `data_prep.py`. Run it like this:

```sh
py scripts\data_prep.py
```

## Things to remember

The following are things to refresh my memory:

### Submitting code via git

Add, commit with message, push to Github.

NOTE: Messages should be in present tense.

```sh
git add .
git commit -m "Add starter files"
git push
```
