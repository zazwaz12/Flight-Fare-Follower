# Git Commands and Workflow

## Making a New Branch
```bash
git branch <branchName>

#Switching Branch
git checkout <branchName>

#Checking Commit Status
git status
```

## Commiting a New Branch (on computer)

On the branch:

```bash
git add .
git commit -m "This is where the comment goes"
#To view the logs:
git log
```

## Merge Conflicts - the Necessity for Pull Requests

HEAD points to the current branch
    should not be working on mstr

## Create the feature branch:
```bash
git branch featureBranch
git checkout featureBranch
git log  # to make sure HEAD = featureBranch
```
## Upload BRANCH ONLY NOT MAIN to GitHub:
```bash
git checkout featureBranch
git add . 
git commit -m "This is the branch update message"
git push origin featureBranch  # then check GitHub to make sure it uploaded
```

## Create a "Pull Request":
* Go to the pull requests tab
* Base is master, Compare is the relevant branch you were working on
* Add a message with the changes made functionally
* Merge feature branch into master/main branch:
* Merge the pull request
* Add a useful comment on the code and resolve any conflicts

## Updating Local Repository with GitHub Repository
```bash
git fetch origin
#since the main branch to combine git fetch n git merge:
git pull origin main
```

### Best Practices

* Avoid using git checkout master
* Reverting Changes
```bash
git reset <hash>  # as shown in the versioning log
#Note: Replace <branchName> and <hash> with actual branch names and commit hashes as needed.
```
UPLOADED through ft. brnch