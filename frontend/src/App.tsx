// General react imports.
import { useMemo, useState } from 'react';

// MRT imports.
import {
  MantineReactTable,
  useMantineReactTable,
  type MRT_ColumnDef,
  type MRT_ColumnFilterFnsState,
  type MRT_ColumnFiltersState,
  type MRT_PaginationState,
  type MRT_SortingState,
} from 'mantine-react-table';

// UI elements.
import { ActionIcon, Tooltip } from '@mantine/core';
import { IconRefresh } from '@tabler/icons-react';

// react-query imports.
import {
  QueryClient,
  QueryClientProvider,
  useQuery,
} from '@tanstack/react-query';

import './App.css'

// Conjunction datatype.
type Conjunction = {
  norad_id_i: number;
  norad_id_j: number;
  tca: string;
  tca_pj: number;
  dca: number;
  relative_speed: number;
};

// Type expected from the conjunctions endpoint in the backend.
type ConjunctionApiResponse = {
  // The set of conjunctions to be displayed in the current page.
  rows: Array<Conjunction>,
  // The total number of conjunctions in the dataframe.
  tot_nrows: number;
}

// Bundle of parameters to be passed to useGetConjunctions(). This contains
// several MRT types describing aspects of the table state such as the sorting
// criteria, pagination, filtering, etc.
type useGetConjunctionsParams = {
  columnFilterFns: MRT_ColumnFilterFnsState;
  columnFilters: MRT_ColumnFiltersState;
  sorting: MRT_SortingState;
  pagination: MRT_PaginationState;
  globalFilter: string;
}

// react-query hook to fetch the list of conjunctions from the backend.
const useGetConjunctions = ({ columnFilterFns, columnFilters, sorting, pagination, globalFilter }: useGetConjunctionsParams) => {
  // API url.
  const url = 'http://localhost:8000/public_conjunctions/'

  // The body for the POST request. Here we are setting all the parameters
  // to be passed to the backend.
  const body = {
    begin: pagination.pageIndex * pagination.pageSize,
    nrows: pagination.pageSize,
    sorting: sorting,
    conjunctions_filter_fns: columnFilterFns,
    conjunctions_filters: columnFilters,
    global_filter: globalFilter
  };

  // Define the function that performs the API call.
  const queryFunction = async () => {
    // Options for the request.
    const requestOptions = {
      method: 'POST',
      // NOTE: it is important to set the correct content type here.
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body)
    };

    // Make the request.
    const response = await fetch(url, requestOptions);

    // Fetch the result
    const responseData = await response.json();

    // Handle errors.
    if (!response.ok) {
      // Get error message from body or default to response status.
      const error = (responseData && responseData.message) || response.status;
      return Promise.reject(error);
    }

    return responseData;
  };

  // NOTE: what this does is essentially adding a few features on top of just
  // calling queryFunction() directly. The important bit for us it the caching behaviour:
  // if the result of a previous invocation of queryFunction() for a given 'body' was already
  // computed, the cached result will be returned.
  return useQuery<ConjunctionApiResponse>({
    // Here's the cache: we need to give a unique name ('conjunctions')
    // and pass the current 'body'.
    queryKey: ['conjunctions', body],
    queryFn: queryFunction,
    // NOTE: see https://github.com/TanStack/query/discussions/6460.
    placeholderData: (prev) => prev,
    // NOTE: do not refetch previously viewed pages until cache is more than 30 seconds old.
    staleTime: 30_000,
  });
};

// Function to create the table of conjunctions.
const ConjunctionsTable = () => {
  // Definition of the columns.
  const columns = useMemo<MRT_ColumnDef<Conjunction>[]>(
    () => [
      {
        accessorKey: 'norad_id_i',
        header: 'Norad ID i',
        columnFilterModeOptions: ['equals'],
        enableFilterMatchHighlighting: true,
      },
      {
        accessorKey: 'norad_id_j',
        header: 'Norad ID j',
        columnFilterModeOptions: ['equals'],
        enableFilterMatchHighlighting: true,
      },
      {
        accessorKey: 'tca',
        header: 'TCA (UTC)',
        // NOTE: the idea here is to regularise the date representation
        // by converting it to Date and then back to ISO format.
        Cell: ({ cell }) => new Date(cell.getValue<string>()).toISOString(),
        enableColumnFilter: false
      },
      {
        accessorKey: 'dca',
        header: 'DCA (km)',
        Cell: ({ cell }) => cell.getValue<Number>().toPrecision(4),
        columnFilterModeOptions: ['greaterThan', 'lessThan', 'between', 'betweenInclusive'],
      },
      {
        accessorKey: 'relative_speed',
        header: 'Rel. speed (km/s)',
        Cell: ({ cell }) => cell.getValue<Number>().toPrecision(4),
        columnFilterModeOptions: ['greaterThan', 'lessThan', 'between', 'betweenInclusive'],
      },
    ],
    [],
  );

  // Manage MRT state that we want to pass to our API.
  const [columnFilters, setColumnFilters] = useState<MRT_ColumnFiltersState>(
    [],
  );
  // Filter modes. We have different defaults depending on the column.
  const [columnFilterFns, setColumnFilterFns] =
    useState<MRT_ColumnFilterFnsState>(
      {
        'norad_id_i': 'equals', 'norad_id_j': 'equals',
        'dca': 'betweenInclusive', 'relative_speed': 'betweenInclusive'
      }
    );
  const [sorting, setSorting] = useState<MRT_SortingState>([]);
  const [pagination, setPagination] = useState<MRT_PaginationState>({
    pageIndex: 0,
    pageSize: 10,
  });
  const [globalFilter, setGlobalFilter] = useState('');

  // Call our custom react-query hook to fetch the data from the backend.
  const { data, isError, isFetching, isLoading, refetch } = useGetConjunctions({
    columnFilterFns,
    columnFilters,
    pagination,
    sorting,
    globalFilter
  });

  // Fetch the conjunctions for the current page and the total
  // number of conjunctions from the response.
  const fetchedConjunctions = data?.rows ?? [];
  const totalRowCount = data?.tot_nrows ?? 0;

  const table = useMantineReactTable({
    columns,
    data: fetchedConjunctions,
    enableColumnFilterModes: true,
    columnFilterModeOptions: [],
    initialState: { showColumnFilters: true, showGlobalFilter: true },
    manualFiltering: true,
    manualPagination: true,
    manualSorting: true,
    mantineToolbarAlertBannerProps: isError
      ? {
        color: 'red',
        children: 'Error loading data',
      }
      : undefined,
    onColumnFilterFnsChange: setColumnFilterFns,
    onColumnFiltersChange: setColumnFilters,
    onPaginationChange: setPagination,
    onSortingChange: setSorting,
    renderTopToolbarCustomActions: () => (
      <Tooltip label="Refresh Data">
        <ActionIcon onClick={() => refetch()}>
          <IconRefresh />
        </ActionIcon>
      </Tooltip>
    ),
    rowCount: totalRowCount,
    state: {
      columnFilterFns,
      columnFilters,
      isLoading,
      pagination,
      showAlertBanner: isError,
      showProgressBars: isFetching,
      sorting,
      globalFilter,
    },
    enableColumnDragging: true,
    enableColumnOrdering: true,
    mantineSearchTextInputProps: {
      placeholder: 'Search satellites',
      sx: { minWidth: '300px' },
    },
    onGlobalFilterChange: setGlobalFilter,
  });

  return <MantineReactTable table={table} />;
};

const queryClient = new QueryClient();

const ConjunctionsQueryProvider = () => (
  <QueryClientProvider client={queryClient}>
    <ConjunctionsTable />
  </QueryClientProvider>
);

export default ConjunctionsQueryProvider;
