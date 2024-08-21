param(
  [string] $userName,
  [string] $ori_token,
  [string] $ori_Repo_link,
  [string] $ori_Repo_name,
  [string] $ori_branch,
  
  [string] $tar_token,
  [string] $tar_Repo_link,
  [string] $tar_branch
)

# Set up creds
$ori_base64AuthInfo = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes(("{0}:{1}" -f $userName,$ori_token)))
$tar_base64AuthInfo = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes(("{0}:{1}" -f $userName,$tar_token)))

# Set up Repo
Write-Host "Cloning Branch $ori_branch from $ori_Repo_link"
git config --global credential.authority basic
git -c http.extraHeader="Authorization: Basic $ori_base64AuthInfo" clone --single-branch --branch $ori_branch $ori_Repo_link
#git config -l  #for debug only
cd $ori_Repo_name

# Set up Repo
Write-Host "Pushing Branch $tar_branch onto $tar_Repo_link"
git -c http.extraHeader="Authorization: Basic $tar_base64AuthInfo" push $tar_Repo_link ${ori_branch}:${tar_branch} --force

Write-Host "Process completed"