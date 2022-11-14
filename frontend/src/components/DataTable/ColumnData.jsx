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
        { field: 'AZ', headerName: 'AZ', flex: 0.5, type: 'number', description: 'Availability Zone ID. For details, please refer to https://docs.aws.amazon.com/ram/latest/userguide/working-with-az-ids.html',
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
        { field: 'Calculator OnDemand Price', headerName: 'OnDemand Price', flex: 1, type: 'number',
            headerAlign: 'center',
            valueGetter: (params) => {
                return params.row['Calculator OnDemand Price'] == -1 ? "N/A" : params.row['Calculator OnDemand Price'];
            }
        },
        { field: 'Calculator Preemptible Price', headerName: 'Preemptible Price', flex: 1, type: 'number',
            headerAlign: 'center',
            valueGetter: (params) => {
                return params.row['Calculator Preemptible Price'] == -1 ? "N/A" : params.row['Calculator Preemptible Price'];
            }
        },
        { field: 'Calculator Savings', headerName: 'Savings', flex: 1, type: 'number',
            headerAlign: 'center',
            valueGetter: (params) => {
                if (!params.row['Calculator OnDemand Price'] || !params.row['Calculator Preemptible Price']) return "N/A"; // 값이 없을 경우 (공백문자, null, undefined) N/A
                else if (params.row['Calculator OnDemand Price'] == -1 || params.row['Calculator Preemptible Price'] == -1) return "N/A";  // 값이 -1일 경우 (string, num...)
                let savings = Math.round((params.row['Calculator OnDemand Price'] - params.row['Calculator Preemptible Price']) / params.row['Calculator OnDemand Price'] * 100)
                return isNaN(savings) ? "N/A" : savings;
            }
        },
        { field: 'VM Instance OnDemand Price', headerName: 'OnDemand Price', flex: 1, type: 'number',
            headerAlign: 'center',
            valueGetter: (params) => {
                return params.row['VM Instance OnDemand Price'] == -1 ? "N/A" : params.row['VM Instance OnDemand Price'];
            }
        },
        { field: 'VM Instance Preemptible Price', headerName: 'Preemptible Price', flex: 1, type: 'number',
            headerAlign: 'center',
            valueGetter: (params) => {
                return params.row['VM Instance Preemptible Price'] == -1 ? "N/A" : params.row['VM Instance Preemptible Price'];
            }
        },
        { field: 'VM Instance Savings', headerName: 'Savings', flex: 1, type: 'number',
            headerAlign: 'center',
            valueGetter: (params) => {
                if (!params.row['VM Instance Preemptible Price'] || !params.row['VM Instance OnDemand Price']) return "N/A"; // 값이 없을 경우 (공백문자, null, undefined) N/A
                else if (params.row['VM Instance Preemptible Price'] == -1 || params.row['VM Instance OnDemand Price'] == -1) return "N/A"; // 값이 -1일 경우 (string, num...)
                let savings = Math.round((params.row['VM Instance OnDemand Price'] - params.row['VM Instance Preemptible Price']) / params.row['VM Instance OnDemand Price'] * 100)
                return isNaN(savings) ? "N/A" : savings;
            }
        },
        {field: 'time', headerName: 'Date', type: 'date', flex: 2, headerAlign: 'center', }
    ];
    const GCPcolumnGroup = [
        {
            groupId: 'Caculator',
            description: '',
            children: [{ field: 'Calculator OnDemand Price' }, {field: 'Calculator Preemptible Price'},{field: 'Calculator Savings'}],
        },
        {
            groupId: 'VM Instance',
            description: '',
            children: [{ field: 'VM Instance OnDemand Price' }, {field: 'VM Instance Preemptible Price'},{field: 'VM Instance Savings'}],
        }
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
        { field: 'savings', headerName: 'Savings (%)', flex: 1.3, type: 'number',
            headerAlign: 'center',
            valueGetter: (params) => {
                if (!params.row.OndemandPrice || !params.row.SpotPrice) return "N/A"; // 값이 없을 경우 (공백문자, null, undefined) N/A
                else if (params.row.OndemandPrice == -1 || params.row.OndemandPrice == -1) return "N/A"; // 값이 -1일 경우 (string, num...)
                let savings = Math.round((params.row.OndemandPrice - params.row.SpotPrice) / params.row.OndemandPrice * 100)
                return isNaN(savings) ? "N/A" : savings;
            }
        },
        { field: 'time', headerName: 'Date', type: 'date', flex: 2, headerAlign: 'center' }
    ];
    return {
        columns,
        GCPcolumnGroup,
        GCPcolumns,
        AZUREcolumns
    }
}

export default ColumnData;