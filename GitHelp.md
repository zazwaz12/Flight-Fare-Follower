# Git Commands and Workflow

## Making a New Branch
```bash
git branch <branchName>
Switching Branch

bash
Copy code
git checkout <branchName>
Checking Commit Status

bash
Copy code
git status
Workflow
On the branch:

bash
Copy code
git add .
git commit -m "This is where the comment goes"
To view the logs:

bash
Copy code
git log
Merge Conflicts - the Necessity for Pull Requests

HEAD points to the current branch
Code Reviews

Create the feature branch:
bash
Copy code
git branch featureBranch
git checkout featureBranch
git log  # to make sure HEAD = featureBranch
Upload to GitHub:
bash
Copy code
git add . 
git commit -m "This is the branch update message"
git push origin master
git checkout featureBranch
git push origin featureBranch  # then check GitHub to make sure it uploaded
Create a "Pull Request":
Go to the pull requests tab
Base is master, Compare is the relevant branch you were working on
Add a message with the changes made functionally
Merge feature branch into master/main branch:
Merge the pull request
Add a useful comment on the code and resolve any conflicts
Updating Local Repository with GitHub Repository

bash
Copy code
git fetch origin
Best Practices

Avoid using git checkout master
Reverting Changes

bash
Copy code
git reset <hash>  # as shown in the versioning log
Note: Replace <branchName> and <hash> with actual branch names and commit hashes as needed.