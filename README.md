# CluedIn.CSharp.Template

Template containing common files and folder structure for creating new C# repositories.

## Simple Usage

1. To create a Git repository for a new C# project follow the steps below

    ```Shell
    # Clone the CSharp template repository and step into the new folder
    git clone https://github.com/CluedIn-io/CluedIn.CSharp.Template.git CluedIn.ProjectName
    cd CluedIn.ProjectName

    # Rename the Git remote to template
    git remote rename origin template
    ```

1. Rename the Visual Studio solution in the root folder

    ```Shell
    ren CluedIn.CSharp.Template.sln CluedIn.ProjectName.sln
    ```

1. Create the CluedIn.ProjectName repository under the CluedIn organization in GitHub

1. Set the new GitHub repository as the _origin_ remote

    ```Shell
    git remote add origin https://github.com/CluedIn-io/CluedIn.ProjectName
    ```

1. Push the content to the new Git repository

    ```Shell
    git push
    ```

Substitute _CluedIn.ProjectName_ for your project name.

## CluedIn Repository Code Migration

To use this repository as a basis for migrating out an existing project from another repository use the instructions provided below.

1. Create a feature branch to perform the code migration in

    ```Shell
    git checkout feature/Migrate-CluedIn-Repository-Code
    ```

1. Copy across code from source repository

1. Convert projects to VS2017 format so that `dotnet` CLI can work with them

    ```PowerShell
    gci . *.csproj -recurse | ForEach-Object { dotnet migrate-2017 migrate $_.FullName }
    ```

1. Remove backup files

    ```PowerShell
    gci . backup* -Directory -recurse | remove-item -recurse -force -verbose
    ```