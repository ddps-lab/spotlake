import React from "react";

const ColumnData = () =>{
    //AWS columns
    const columns = [
        { field: 'id', headerName: 'ID', flex: 1, filterable: false, hide: true },
        { field: 'InstanceType', headerName: 'InstanceType', flex: 1,
            valueGetter: (params) => {
                return params.row.InstanceType == -1 ? "N/A" : params.row.InstanceType;
            }
        },
        { field: 'Region', headerName: 'Region', flex: 1, headerAlign: 'center',
            valueGetter: (params) => {
                return params.row.Region == -1 ? "N/A" : params.row.Region;
            }
        },
        { field: 'AZ', headerName: 'AZ', flex: 0.5, description: 'Availability Zone ID. For details, please refer to https://docs.aws.amazon.com/ram/latest/userguide/working-with-az-ids.html',
            headerAlign: 'center',
            valueGetter: (params) => {
                return params.row.AZ == -1 ? "N/A" : params.row.AZ;
            }
        },
        { field: 'SPS', headerName: 'Availability', flex: 1.3, description: 'In AWS, it is Spot Placement Score. For details, please refer to https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/spot-placement-score.html', type: 'number',
            headerAlign: 'center',
            valueGetter: (params) => {
                return params.row.SPS == -1 ? "N/A" : params.row.SPS;
            }
        },
        { field: 'IF', headerName: 'Interruption Ratio', flex: 1.3, description: 'In AWS, it is Interruption-free score. For details, please refer to “Frequency of interruption” in https://aws.amazon.com/ec2/spot/instance-advisor/', type: 'number',
            headerAlign: 'center',
            valueGetter: (params) => {
                return params.row.IF == -1 ? "N/A" : params.row.IF;
            }
        },
        { field: 'SpotPrice', headerName: 'SpotPrice ($)', type: 'number', flex: 1.3,
            headerAlign: 'center',
            valueGetter: (params) => {
                return params.row.SpotPrice == -1 ? "N/A" : params.row.SpotPrice;
            }
        },
        { field: 'Savings', headerName: 'Savings (%)', flex: 1.3, type: 'number',
            headerAlign: 'center',
            valueGetter: (params) => {
                if (!params.row.OndemandPrice || !params.row.SpotPrice) return "N/A"; // 값이 없을 경우 (공백문자, null, undefined) N/A
                else if (params.row.OndemandPrice == -1 || params.row.SpotPrice == -1) return "N/A"; // 값이 -1일 경우 (string, num...)
                let savings = Math.round((params.row.OndemandPrice - params.row.SpotPrice) / params.row.OndemandPrice * 100)
                return isNaN(savings) ? "N/A" : savings;
            }
        },
        // { field: 'SpotRank', headerName: 'SpotRank', flex: 1, type: 'number',
        //   valueGetter: (params) =>{
        //     let rank = (params.row.Savings / 100.0) * (alpha * params.row.SPS + (1-alpha) * params.row.IF)
        //     return rank.toFixed(2);
        //   }
        // },
        {field: 'Date', headerName: 'Date', type: 'date', flex: 2,
            headerAlign: 'center',
            valueGetter: (params) =>{
                if (params.row.TimeStamp) {
                    const date = new Date(params.row.TimeStamp);
                    let year = date.getFullYear();
                    let month = "0" + (date.getMonth() + 1);
                    let day = "0" + date.getDate();
                    let hour = date.getHours();
                    let min = date.getMinutes();
                    return year + '-' + month.substr(-2) + '-' + day.substr(-2) + ' ' + "0" + hour + ':' + "0" + min;
                } else return params.row.time;
            }},
    ];
    //GCP columns
    const GCPcolumns = [
        { field: 'id', headerName: 'ID', flex: 1, filterable: false, hide: true},
        { field: 'InstanceType', headerName: 'InstanceType', flex: 1.3,
            headerAlign: 'center',
            valueGetter: (params) => {
                return params.row.InstanceType == -1 ? "N/A" : params.row.InstanceType;
            }
        },
        { field: 'Region', headerName: 'Region', flex: 1,
            headerAlign: 'center',
            valueGetter: (params) => {
                return params.row.Region == -1 ? "N/A" : params.row.Region;
            }
        },
        { field: 'OnDemand Price', headerName: 'OnDemand Price', flex: 1, type: 'number',
            headerAlign: 'center',
            valueGetter: (params) => {
                return params.row['OnDemand Price'] == -1 ? "N/A" : params.row['OnDemand Price'];
            }
        },
        { field: 'Spot Price', headerName: 'Spot Price', flex: 1.3, type: 'number',
            headerAlign: 'center',
            valueGetter: (params) => {
                return params.row['Spot Price'] == -1 ? "N/A" : params.row['Spot Price'];
            }
        },
        { field: 'Savings', headerName: 'Savings (%)', flex: 1.3, type: 'number',
            headerAlign: 'center',
            valueGetter: (params) => {
                if (!params.row['OnDemand Price'] || !params.row['Spot Price']) return "N/A"; // 값이 없을 경우 (공백문자, null, undefined) N/A
                else if (params.row['OnDemand Price'] == -1 || params.row['Spot Price'] == -1) return "N/A"; // 값이 -1일 경우 (string, num...)
                let savings = Math.round((params.row['OnDemand Price'] - params.row['Spot Price']) / params.row['OnDemand Price'] * 100)
                return isNaN(savings) ? "N/A" : savings;
            }
        },
        {field: 'time', headerName: 'Date', type: 'date', flex: 2, headerAlign: 'center', }
    ];
    //AZURE columns
    const AZUREcolumns = [
        { field: 'id', headerName: 'ID', flex: 1, filterable: false, hide: true},
        { field: 'InstanceTier', headerName: 'InstanceTier', flex: 1 ,
            headerAlign: 'center',
            valueGetter: (params) => {
                return params.row.instanceTier == -1 ? "N/A" : params.row.InstanceTier;
            }
        },
        { field: 'InstanceType', headerName: 'InstanceType', flex: 1,
            headerAlign: 'center',
            valueGetter: (params) => {
                return params.row.instanceType == -1 ? "N/A" : params.row.InstanceType;
            }
        },
        { field: 'Region', headerName: 'Region', flex: 1,
            headerAlign: 'center',
            valueGetter: (params) => {
                return params.row.region == -1 ? "N/A" : params.row.Region;
            }
        },
        { field: 'OndemandPrice', headerName: 'OndemandPrice', flex: 1, type: 'number',
            headerAlign: 'center',
            valueGetter: (params) => {
                return params.row.ondemandPrice == -1 ? "N/A" : params.row.OndemandPrice;
            }
        },
        { field: 'SpotPrice', headerName: 'SpotPrice', flex: 1.3, type: 'number',
            headerAlign: 'center',
            valueGetter: (params) => {
                return params.row.spotPrice == -1 ? "N/A" : params.row.SpotPrice;
            }
        },
        { field: 'IF', headerName: 'IF', flex: 1.3, type: 'number', description: 'Interruption Free (IF) score refers to the interruption ratio offered by Azure. Please check the score calculation in the About page. For IF score being "N/A", the Azure portal might provide valid score value.',
            headerAlign: 'center',
            valueGetter: (params) => {
                return params.row.IF == -1 ? "N/A" : params.row.IF;
            }
        },
        { field: 'savings', headerName: 'Savings (%)', flex: 1.3, type: 'number',
            headerAlign: 'center',
            valueGetter: (params) => {
                if (!params.row.OndemandPrice || !params.row.SpotPrice) return "N/A"; // 값이 없을 경우 (공백문자, null, undefined) N/A
                else if (params.row.OndemandPrice == -1 || params.row.SpotPrice == -1) return "N/A"; // 값이 -1일 경우 (string, num...)
                let savings = Math.round((params.row.OndemandPrice - params.row.SpotPrice) / params.row.OndemandPrice * 100)
                return isNaN(savings) ? "N/A" : savings;
            }
        },
        { field: 'time', headerName: 'Date', type: 'date', flex: 2, headerAlign: 'center' }
    ];
    return {
        columns,
        GCPcolumns,
        AZUREcolumns
    }
}

export default ColumnData;
